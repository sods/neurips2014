import time
import os
import re
import sys
import numpy as np
import pickle
import pandas as pd
import sqlite3

import openpyxl # Requires python-openpyxl
from lxml import etree # for reading from CMT

from HTMLParser import HTMLParser
# General set up.

from pods.util import download_url
from pods.notebook import display_url

# interface to google docs
import pods
from config import *

conf_short_name = config.get('conference', 'short_name')
conf_year = config.get('conference', 'year')
program_chair_email = config.get('conference', 'chair_email')
program_chair_gmails = config.get('conference', 'chair_gmails').split(';')
cmt_data_directory = os.path.expandvars(config.get('cmt', 'export_directory'))
buddy_pair_key = os.path.expandvars(config.get('google docs', 'buddy_pair_key'))
global_results_key = os.path.expandvars(config.get('google docs', 'global_results_key'))

# When recruiting reviewers we add in people who area chaired at ICML since 2008, at NIPS since 2001 and at AISTATS since 2011.
# Conferences with area chair information stored
recorded_conferences = {'icml': [2008, 2009, 2010, 2011, 2012, 2013, 2014],
                        'nips': [2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013],
                        'aistats' : [2011, 2012, 2013, 2014]}

# Helper function for formatting strings.
def my_format(num,length=3):
    return str(num)[:length+1]

# HTML Stripper from this stackoverflow post: http://stackoverflow.com/questions/753052/strip-html-from-strings-in-python
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def split_names(names):
    """
    If we have a spreadsheet that stores the name only, this function
    splits the name into first name, middle initials and last name, which
    is the format that CMT expects. Information about name splits is taken
    from the file 'name_splits.txt'
    """
    with open('name_splits.txt') as f:
        name_splits = f.read().splitlines()
    firstname = pd.Series(index=names.index)
    middlenames = pd.Series(index=names.index)
    lastname = pd.Series(index=names.index)
    for ind in names.index:
        split_name = False
        for split in name_splits:
            if  names[ind] == split.replace('|', ' '):
                cand_names = split.split('|')
                split_name = True
                break

        if not split_name:
            cand_names = names[ind].split(' ')

        firstname[ind] = cand_names[0].strip()
        lastname[ind] = cand_names[-1].strip()

        if len(names)>2:
            middlenames[ind] = ' '.join(cand_names[1:-1])
        else:
            middlenames[ind] = ''


    return firstname, middlenames, lastname

# How to map spreadsheet column titles to data base columns
default_mapping = {}
default_mapping['FirstName'] ='FirstName'
default_mapping['MiddleNames'] = 'MiddleNames'
default_mapping['LastName'] = 'LastName'
default_mapping['Institute'] = 'Institute'
default_mapping['Email'] = 'Email'
default_mapping['ScholarID'] = 'ScholarID'
default_mapping['Nominator'] = 'Nominator'


class review_report:
    """
    Class that looks at calibrated reviews and generates text reports or email reports based on review sumaries.
    """
    def __init__(self, filename=None,
                 calibrated_reviews=None,
                 attention_scoring=None,
                 light_grey=[0.1, 0.9],
                 firm_grey=[0.3, 0.7],
                 expected_reviews=3,
                 short_review_percentile=5.,
                 very_short_review_percentile=1.):

        if calibrated_reviews is None and filename is not None:
            calibrated_reviews = pd.io.parsers.read_csv(os.path.join(cmt_data_directory, filename),
                                                        dtype={'PaperID':object})
            calibrated_reviews.set_index(keys='PaperID', inplace=True)
            calibrated_reviews.fillna('', inplace=True)
        elif ((filename is None and calibrated_reviews is None) or
              (filename is not None and calibrated_reviews is not None)):
            raise ValueError("You need to provide either filename or calibrated_reviews as keyword arguments")

        self.reviews=calibrated_reviews
        self.short_review_percentile = short_review_percentile
        self.very_short_review_percentile = very_short_review_percentile
        self.comment_length_very_low_threshold = self.reviews.comment_length.quantile(self.very_short_review_percentile/100.)
        self.comment_length_low_threshold = self.reviews.comment_length.quantile(self.short_review_percentile/100.)
        self.high_impact_threshold = 1.5
        self.light_grey_area = light_grey
        self.firm_grey_area = firm_grey
        self.expected_reviews = expected_reviews
        if attention_scoring is None:
            self.attention_scoring = {'one_review':40,
                                      'too_few_reviews':14,
                                      'firm_grey_area':7,
                                      'light_grey_area':3,
                                      'likely_accept':2,
                                      'talk':7,
                                      'very_large_span':5,
                                      'large_span':2,
                                      'very_low_confidence':5,
                                      'low_confidence':2,
                                      'very_short_review':7,
                                      'short_review':4}
        else:
            self.attention_scoring=attention_scoring

    def email_html_comments(self, sendto_dict,
                            intro=None,
                            closing=None,
                            subject=None,
                            attention_threshold=0,
                            cc_list = [program_chair_email],
                            rankby='attention_score'):
        """Email html comments to area chairs."""

        if subject is None:
            subject = conf_short_name + ": Report on Papers which May Need Attention"
        if intro is None:
            intro = """Hi,<br><br>

            This is an automated report that tries to identify problems with
            papers that need attention. You may already be on top of these
            issues, but this report may still be helpful.<br><br>

            The report ranks papers by an 'attention score' to try and order
            which papers require most attention.<br><br>

            Calibrated quality scores are scores that take the estimated
            'reviewer bias' into account. The probability of accept is based
            on a probabilistic model that accounts for the reviewer bias we
            estimated and the associated uncertainty.<br><br>"""

        if closing is None:
            closing = """<br><br>

            Regards,<br><br>


            """ + chair_informal_names + "<br>\n" + conf_short_name + ' ' + conf_year + " Program Chairs"

        for email, papers in sendto_dict.iteritems():
            print("Sending mails summarizing papers", ', '.join(papers), 'to', email)
        ans = raw_input('Are you sure you want to send mails (Y/N)?')
        if ans=='Y':
            mailer = gmail.email(gmail_username=gmail_account)
            for email, papers in sendto_dict.iteritems():
                body = ''
                for id, report in self.attention_report.loc[papers][self.attention_report.loc[papers].attention_score>attention_threshold].sort(columns=rankby, ascending=False).iterrows():
                    body += report.comments
                if len(body)>0:
                    email_text = intro + body + closing
                    mailer.send(session=session,recipient=email, cc=cc_list, body=email_text, subject=subject, reply_to=program_chair_email)

    def _repr_html_(self):
        """Return an HTML representation of the entire report."""
        html = ''
        max_return = 50
        count = 0
        for id, paper in self.attention_report.sort(columns='attention_score', ascending=False).iterrows():
            html += paper.comments
            count += 1
            if count > 50:
                html += '<br><br><b>Report continues, only 50 papers shown ...</b>'
                return html
        return html

    def attention_score(self, paper, issues=None):
        """Compute the attention score for a given paper."""
        if issues is None:
            issues = self.issues(paper)

        attention_score = 0
        for issue in issues:
            attention_score += self.attention_scoring[issue.split('+')[0]]
        return attention_score

    def generate_comments(self):
        """Generate a paragraph of comments for each paper."""
        self.generate_html_comments()
        self.comments = {}
        for paper, comments in self.html_comments.iteritems():
            self.comments[paper] = strip_tags(self.html_comments[paper])

    def generate_html_comments(self):
        """Generate html comments for each paper."""
        self.html_comments={}
        attention_scores={}
        for paper in set(self.reviews.index):
            p = self.reviews.loc[paper]
            issues = self.issues(paper)
            attention_score = self.attention_score(paper, issues)
            html_comment = self.generate_html_comment(paper, issues)
            if type(p) is pd.DataFrame:
                title = list(p.Title)[0]
            else:
                title = p.Title
            html_comment = '\n<h3>Paper '  + paper + ' ' + title + '</h3>\n\n' + html_comment + '<br>\nAttention Score: ' + str(attention_score)

            self.html_comments[paper] = html_comment
            attention_scores[paper] = attention_score
        self.attention_report = pd.DataFrame({'comments': pd.Series(self.html_comments), 'attention_score':pd.Series(attention_scores)})
        self.attention_report.sort(columns='attention_score', inplace=True, ascending=False)


    def spreadsheet_comments(self):
        """Generate comments suitable for placing in a spreadsheet."""
        comments = {}
        attention_scores = {}
        quality_scores = {}
        confidence_scores = {}
        calibrated_quality_scores = {}
        impact_scores = {}
        reviewer_list = {}
        prob_accept = {}
        paper_title = {}
        notes = {}
        accept = {}
        talk = {}
        spotlight = {}
        discussions = {}
        for paper in set(self.reviews.index):
            p = self.reviews.loc[paper]
            attention_scores[paper] = self.attention_score(paper)
            quality_scores[paper] = ','.join(map(str,p.Quality))
            confidence_scores[paper] = ','.join(map(str,p.Conf))
            impact_scores[paper] = ','.join(map(str,p.Impact))
            calibrated_quality_scores[paper] = ','.join(map(my_format,p.CalibratedQuality))
            comments[paper] = self.summary_comment(paper)
            reviewer_names = []
            for paperid, review in p.iterrows():
                reviewer_names.append(review.FirstName + ' ' + review.LastName)
            reviewer_list[paper] = ','.join(reviewer_names)
            paper_title[paper] = p.Title[0]
            prob_accept[paper] = my_format(p.AcceptProbability[0],5)
            notes[paper] = ''
            talk[paper] = ''
            spotlight[paper] = ''
            accept[paper] = ''
            discussions[paper] = p['Number Of Discussions'][0]
        self.attention_report = pd.DataFrame({'comments': pd.Series(comments),
                                              'attention_score':pd.Series(attention_scores),
                                              'quality':pd.Series(quality_scores),
                                              'calibrated_quality': pd.Series(calibrated_quality_scores),
                                              'confidence':pd.Series(confidence_scores),
                                              'impact':pd.Series(impact_scores),
                                              'reviewers':pd.Series(reviewer_list),
                                              'paper_title':paper_title,
                                              'prob_accept':prob_accept,
                                              'notes':notes,
                                              'talk':talk,
                                              'spotlight':spotlight,
                                              'accept':accept,
                                              'discussions':discussions})

        column_presentation_order = ['paper_title',
                                     'prob_accept',
                                     'attention_score',
                                     'discussions',
                                     'reviewers',
                                     'quality',
                                     'calibrated_quality',
                                     'confidence',
                                     'impact',
                                     'comments',
                                     'notes',
                                     'accept',
                                     'talk',
                                     'spotlight']

        column_sort_order = ['attention_score', 'prob_accept']

        self.attention_report = self.attention_report[column_presentation_order]
        self.attention_report.sort(column_sort_order, inplace=True,ascending=False)

    def issues(self, paper):
        """Identify the potential issues with a given paper."""

        paper = str(paper)
        p = self.reviews.loc[paper]
        issues = []

        # Check for requisite number of reviews
        num_revs = list(self.reviews.index).count(paper)
        if num_revs<self.expected_reviews:
            if num_revs < 2:
                issues.append('one_review')
                return issues
            else:
                issues.append('too_few_reviews')
        prob = p.AcceptProbability.mean()

        # Check for whether the paper is borderline
        if prob >= self.light_grey_area[0] and prob < self.light_grey_area[1]:
            if prob >= self.firm_grey_area[0] and prob<self.firm_grey_area[1]:
                issues.append('firm_grey_area')
            else:
                issues.append('light_grey_area')

        # Check if paper is likely to be accepted
        if prob >= self.light_grey_area[1]:
            issues.append('likely_accept')

        # Check if paper is high impact and likely to be accepted
        impact = p.Impact.mean()
        if impact>=self.high_impact_threshold and prob >= self.firm_grey_area[1]:
            issues.append('talk')

        # Check the span of the reviews
        review_span = p.Quality.max() - p.Quality.min()
        if review_span > 2:
            if review_span>3:
                issues.append('very_large_span')
            else:
                issues.append('large_span')

        # Check for reviewer confidence and review length.
        if num_revs > 1:
            for paperid, review in p.iterrows():
                if review.Conf < 3:
                    if review.Conf < 2:
                        issues.append('very_low_confidence'+'+'+ review.Email)
                    else:
                        issues.append('low_confidence' + '+' + review.Email)
                if review.comment_length < self.comment_length_low_threshold:
                    if review.comment_length < self.comment_length_very_low_threshold:
                        issues.append('very_short_review'+'+'+review.Email)
                    else:
                        issues.append('short_review'+'+'+review.Email)
        return issues

    def base_comments(self, paper):
        """Given general comments about the paper, ignoring specific issues."""
        paper = str(paper)
        p = self.reviews.loc[paper]

        if type(p) is pd.DataFrame: # there has to be a better way of doing this! loc returns string or data frame depending on number of reviewers of paper.
            base_comments = 'Quality scores: ' + ', '.join(map(str,p.Quality)) + '<br>\n'
            base_comments += 'Calibrated quality scores: ' + ', '.join(map(my_format,p.CalibratedQuality)) + '<br>\n'
            base_comments += 'Confidence scores: ' + ', '.join(map(str, p.Conf)) + '<br>\n'
            base_comments += 'Impact scores: ' + ', '.join(map(str, p.Impact)) + '<br>\n'

        else:
            base_comments = 'Quality scores: ' + str(p.Quality) + '<br>\n'
            base_comments += 'Calibrated quality scores: ' + my_format(p.CalibratedQuality) + '<br>\n'
            base_comments += 'Confidence scores: ' + str(p.Conf) + '<br>\n'
            base_comments += 'Impact scores: ' + str(p.Impact) + '<br>\n'

        base_comments += "<br>\nSome things to consider:<br>\n"
        prob = p.AcceptProbability.mean()
        base_comments += "Accept probability for this paper is <b>" + my_format(100*prob) + '%</b>.<br>\n'
        return base_comments

    def generate_html_comment(self, paper, issues=None):
        """ Generate html formatted comments for a specific paper."""

        # The comments dictionary declares the comments to be used.
        comments = {}
        comments['one_review'] = "This paper only has <b>ONE REVIEW</b>!<br>\nYou need to sort that out as soon as possible!<br>\n"
        comments['too_few_reviews'] = "This paper only has {num_revs} reviews.<br>\n"
        comments['firm_grey_area'] = "The paper is firmly in the <b>grey area</b> and will need discussion at teleconference.<br>\n"
        comments['light_grey_area'] = "The paper may be in the <b>grey area</b> and may need discussion at teleconference.<br>\n"
        comments['likely_accept'] = ""
        comments['talk'] = "This paper is likely to be accepted and is currently rated high impact, <b>would it make an appropriate talk or spotlight</b>?<br>\n"
        span = "Difference between max and minimum review score is {review_span}."
        comments['very_large_span'] = span  + ' This is a <b>very large span</b>, the reviewers need to try and discuss the reason for their differences of opinion. If it is resolved scores should be modified to reflect this.<br>\n'
        comments['large_span'] = span + ' This is a large span. Reviews should try and discuss and resolve (adjusting scores if necessary).<br>\n'
        comments['very_low_confidence'] = "Reviewer {reviewer} only has confidence of {reviewer_confidence}.<br>\n"
        comments['low_confidence'] = "Reviewer {reviewer} only has confidence of {reviewer_confidence}.<br>\n"
        reviewer_length = "Reviewer {reviewer} only has a review of length {comment_length} characters."
        review = "It reads as follows:<br>\n<quote>{comment}</quote><br>\n"

        comments['very_short_review'] = reviewer_length + " This is in the <b>shortest " + str(self.very_short_review_percentile) + "% percentile</b> of all our reviews.<br>\n" + review
        comments['short_review'] = reviewer_length + " This is in the <b>shortest " + str(self.short_review_percentile) + "% percentile</b> of all our reviews.<br>\n" + review

        base_comments = self.base_comments(paper)
        return base_comments + self.comment(paper, comments, issues)

    def summary_comment(self, paper, issues=None):
        """ Generate short summary comment for a specific paper. These comments are suitable for spreadsheet entry."""

        comments = {}
        comments['one_review'] = "ONE REVIEW! "
        comments['too_few_reviews'] = "{num_revs} reviews. "
        comments['firm_grey_area'] = "Firm grey area. "
        comments['light_grey_area'] = "Outer-grey area. "
        comments['likely_accept'] = "Likely accept. "
        comments['talk'] = "Talk or Spotlight? "
        span = "Difference between max and minimum review score is {review_span}. "
        comments['very_large_span'] = "Large review span of {review_span}. "
        comments['large_span'] = "Review span of {review_span}. "
        comments['very_low_confidence'] = "{reviewer} confidence is {reviewer_confidence}. "
        comments['low_confidence'] = "{reviewer} confidence is {reviewer_confidence}. "
        reviewer_length = "{reviewer} only has a review of length {comment_length} characters."
        review = "It reads as follows:\n{comment}\n"

        comments['very_short_review'] = "{reviewer} comments only {comment_length} chars long. "
        comments['short_review'] = "{reviewer} comments only {comment_length} chars long. "
        return self.comment(paper, comments, issues)


    def comment(self, paper, comment_dict, issues=None):
        """Generate comments for given paper given a dictionary of comments for specific issues."""
        if issues is None:
            issues = self.issues(paper)
        paper = str(paper)
        p = self.reviews.loc[paper]
        num_revs = list(self.reviews.index).count(paper)
        review_span = p.Quality.max() - p.Quality.min()
        review_tag = {}
        review_comments = {}
        review_confidence = {}
        if type(p) is pd.DataFrame:
            for paperid, review in p.iterrows():
                review_tag[review.Email] = review.FirstName + ' ' + review.LastName + ' (' + review.Email + ')'
                review_comments[review.Email] = review.Comments
                review_confidence[review.Email] = review.Conf
        else:
            review_tag[p.Email] = p.FirstName + ' ' + p.LastName + ' (' + p.Email + ')'
            review_comments[p.Email] = p.Comments
            review_confidence[p.Email] = p.Conf
        comment = ''
        for issue in issues:
            s = issue.split('+')
            if len(s)>1:
                reviewer = s[1]
                c = comment_dict[s[0]].format(reviewer=review_tag[reviewer],
                                          comment=review_comments[reviewer],
                                          comment_length=len(review_comments[reviewer]),
                                          reviewer_confidence = review_confidence[reviewer],
                                          num_revs=num_revs,
                                          review_span=review_span)
            else:
                c = comment_dict[s[0]].format(num_revs=num_revs, review_span=review_span)
            comment += c
        return comment

class reviewers:
    """
    Reviewer class that combines information from the local data base
    and exports from CMT on the reviewer subject areas to characterize the
    reviewers for paper matching
    """
    def __init__(self, directory=None, filename='users.xls', subject_file='Reviewer Subject Areas.xls'):
        if directory is None:
            directory = cmt_data_directory
        self.directory = directory
        self.subjects = {}
        self.load(filename=filename)
        print("Loaded Users.")
        self.load_subjects(filename=subject_file)
        print("Loaded Reviewer Subjects.")

    def load(self, filename='users.xls', localdb='reviewers.db'):
        a = xl_read(filename=filename, header=2, index_col='Email', dataframe=True, lower_index=True)
        cmt_users = a.items
        # Now load in the local store of information
        con = sqlite3.connect(os.path.join(cmt_data_directory, localdb))
        local_users = pd.read_sql('SELECT * from Reviewers', con, index_col='Email')

        # Build a user data base which has the shared info
        self.users = cmt_users.join(local_users, how='inner', rsuffix='_a')

    def load_subjects(self, filename='Reviewer Subject Areas.xls'):
        """Load the reviewer's chosen subject areas from the CMT export file."""
        data = xl_read(filename=os.path.join(self.directory, filename), index_col='Selected Subject Area', header=2, dataframe=True, worksheet_number=1)
        data.items.reset_index(inplace=True)
        #reviewer_subject.replace(to_replace
        data.items['index'] = data.items.index
        self.subjects = {}
        stati = ['Primary', 'Secondary']
        for status in stati:
            self.subjects[status] = data.items.pivot(index='index', columns='Email', values='Primary or Secondary')
            self.subjects[status].replace(to_replace=status, value=1, inplace=True)
            self.subjects[status].replace(to_replace=list(set(stati) - set([status])), value=[0], inplace=True)
            self.subjects[status].fillna(0, inplace=True)
            self.subjects[status].columns = map(str.lower, self.subjects[status].columns)

class papers:
    """
    Paper class that loads information from CMT about the papers'
    subject areas for use in paper to reviewer matching
    """
    def __init__(self, directory=None, filename='Papers.xls'):
        if directory is None:
            directory = cmt_data_directory
        self.directory = directory
        self.subjects = {}
        self.load(filename)
        print("Loaded Papers.")
        self.load_subjects()
        print("Loaded Paper Subjects.")

    def load(self, filename='Papers.xls'):
        """Load in the information about the papers, abstracts, titles, authors etc from CMT exports. `Submissions -> View Active Papers -> Export -> Metadata as Excel`"""
        a = xl_read(filename=filename, header=2, index_col='ID', dataframe=True)
        self.papers = a.items

    def load_subjects(self, filename = 'Paper Subject Areas.xls'):
        """Load paper subject areas from a CMT export file."""
        data = xl_read(filename=os.path.join(self.directory, filename), index_col='Paper ID', dataframe=True, worksheet_number=1)
        data.items.reset_index(inplace=True)
        data.items.rename(columns={'index':'Paper ID'}, inplace=True)
        #reviewer_subject.replace(to_replace
        self.subjects = {}
        stati = ['Primary', 'Secondary']
        for status in stati:
            self.subjects[status] = data.items.pivot(index='Selected Subject Area', columns='Paper ID', values='Primary or Secondary')
            self.subjects[status].replace(to_replace=status, value=1, inplace=True)
            self.subjects[status].replace(to_replace=list(set(stati) - set([status])), value=[0], inplace=True)
            self.subjects[status].fillna(0, inplace=True)

class similarities:
    """
    Similarities class, given a papers class object in submissions and
    a reviewers class object as reviewers it computes the similarity
    measures by loading in bids and TPMS scores and matching reviewers
    to papers by subject similarities. It is then used in an
    allocation object to perform paper allocation.
    """
    def __init__(self, submissions, reviewers, directory=None):
        if directory is None:
            directory = cmt_data_directory
        self.directory = directory

        self.reviewers = reviewers
        self.submissions = submissions
        # check that all subjects are in both reviewers and papers.
        self.subjects = list(set(self.reviewers.subjects['Primary'].index)
                             | set(self.reviewers.subjects['Secondary'].index)
                             | set(self.submissions.subjects['Primary'].index)
                             | set(self.submissions.subjects['Secondary'].index))

        for subjects in [self.reviewers.subjects, self.submissions.subjects]:
            for group in ['Primary', 'Secondary']:
                missing_subjects = list(set(self.subjects)
                                        -set(subjects[group].index))
                for subject in missing_subjects:
                    vals = np.zeros(subjects[group].shape[1])
                    subjects[group].loc[subject] = vals


        self.load_tpms()
        print("Loaded TPMS scores")
        self.load_bids()
        print("Loaded bids")

        # TAKE CARE OF MISSING TPMS ROWS - AK
        diff_index = self.bids.index.difference(self.affinity.index)
        for idx in diff_index:
            self.affinity.loc[idx] = 0

        self.compute_subject_similarity()
        self.compute_scores()


    def load_bids(self, filename='Bids.txt'):
        """Load in Bids information. This is obtained through the `Assignments
        & Conflicts -> Automatic Assignment Wizard`. You need to go through
        the wizard process almost until the end. Then select `Export Data for
        Custom Assignment`. Choose the Tab Delimited format and you will
        download a file `Bids.txt`."""
        self.bids = pd.read_csv(os.path.join(self.directory, filename), delimiter='\t', index_col=False, converters={'Email':str.lower, 'PaperID':str})
        self.bids = self.bids.pivot(index='PaperID', columns='Email', values='BidValue') # Moves the column records into a matrix (with lots of misisng values)
        self.bids.replace(to_replace=0, value=-1, inplace=True)
        self.bids.replace(to_replace=1, value=-0.5, inplace=True)
        self.bids.replace(to_replace=2, value=0.5, inplace=True)
        self.bids.replace(to_replace=3, value=1, inplace=True)
        self.bids.fillna(0, inplace=True)
        self.reviewers_missing_bids = list(set(self.reviewers.users[self.reviewers.users['IsReviewer']=='Yes'].index) - set(self.bids.columns))
        for reviewer in self.reviewers_missing_bids:
            self.bids[reviewer.strip()] = 0.
        self.papers_missing_bids = list(set(self.submissions.papers.index)-set(self.bids.index))
        for paper in self.papers_missing_bids:
            self.bids.loc[paper] = 0.


    def load_tpms(self, filename = 'External Matching Scores(Toronto Paper Matching System).txt'):
        """Load in TPMS information. If you are working with Laurent Charlin
        and TPMS you may have access to the Toronto paper matching
        scores. They are obtained byy first running the match `More ->
        External Reviewer Matching -> Submit Papers for Reviewer
        Matching`. And then you can export the data through the
        `Assignments & Conflicts -> Automatic Assignment Wizard`. You
        need to go through the wizard process almost until the
        end. Then select `Export Data for Custom Assignment`. Choose
        the Tab Delimited format and you will download a file
        `External Matching Scores(Toronto Paper Matching System).txt`.

        """

        self.affinity = pd.read_csv(os.path.join(self.directory, filename), delimiter='\t', index_col=False, na_values=['N/A'], converters={'PaperID':str}).fillna(0)
        self.affinity.set_index(['PaperID'], inplace=True)
        self.affinity.columns = map(str.lower, self.affinity.columns)
        for reviewer in list(set(self.reviewers.users[self.reviewers.users['IsReviewer']=='Yes'].index) - set(self.affinity.columns)):
            self.affinity[reviewer.strip()] = 0.
        #data = xl_read(, index_col='Paper ID', dataframe=True)
        #affinity = data.items
        # Scale affinities to be between 0 and 1.
        self.affinity -= self.affinity.values.min()
        self.affinity /= self.affinity.values.max()


    def compute_subject_similarity(self, alpha=0.5):
        """Compute the similarity between submissions and reviewers by subject
        keyword. Similarities are computed on the basis of keyword
        similarity using primary and secondary keyword matches.
        :param alpha: gives the weighting between primary and secondary keyword match.
        :type alpha: float

        """
        self._sim = {}

        self._sim['Primary'] = pd.DataFrame(np.dot(self.submissions.subjects['Primary'].T, self.reviewers.subjects['Primary']),
                                      index=self.submissions.subjects['Primary'].columns,
                                      columns=self.reviewers.subjects['Primary'].columns)
        self._sim['Secondary'] = pd.DataFrame(np.dot((self.submissions.subjects['Primary'].values + self.submissions.subjects['Secondary'].values).T,
                                               (self.reviewers.subjects['Primary'].values + self.reviewers.subjects['Secondary'])),
                                      index=self.submissions.subjects['Primary'].columns,
                                      columns=self.reviewers.subjects['Primary'].columns)
        self._sim['Secondary'] = (1/np.sqrt(self.reviewers.subjects['Secondary'].sum(axis=0)+1))*self._sim['Secondary']
        self._sim['Secondary'] = ((1/np.sqrt(self.submissions.subjects['Secondary'].sum(axis=0)+1))*self._sim['Secondary'].T).T
        self.subject_similarity = alpha*self._sim['Primary'] + (1-alpha)*self._sim['Secondary']

    def compute_scores(self, alpha = 0.5, b=1.5):
        """Combine TPMS, subject matching and bids into an overal score."""
        self.scores = (alpha*self.affinity + (1-alpha)*self.subject_similarity)
        self.scores = self.scores*b**self.bids

class assignment_diff:
    """
    Stores the difference between two assignments. This is useful for
    finding reviewers who have gained allocations or lost allocations
    between two different assignments. To use it you will need to
    download and store assignment allocation files from CMT regularly.
    """
    def __init__(self, assignment1, assignment2):
        self.gain_paper = {}
        self.loss_paper = {}
        self.gain_reviewer = {}
        self.loss_reviewer = {}

        for reviewer_type in ['reviewer', 'metareviewer']:
            self.loss_reviewer[reviewer_type] = {}
            self.gain_reviewer[reviewer_type] = {}
            papers = set(assignment1.assignment_paper[reviewer_type]) & set(assignment2.assignment_paper[reviewer_type])
            for paper in papers:
                if paper not in assignment2.assignment_paper[reviewer_type]:
                    self.gain_paper[paper] = assignment1.assignment_paper[reviewer_type][paper]

                elif paper not in assignment1.assignment_paper[reviewer_type]:
                    self.loss_paper[paper] = assignment1.assignment_paper[reviewer_type][paper]
                else:
                    diff = list(set(assignment2.assignment_paper[reviewer_type][paper])-set(assignment1.assignment_paper[reviewer_type][paper]))
                    if len(diff)>0:
                        self.gain_paper[paper] = diff
                    diff = list(set(assignment1.assignment_paper[reviewer_type][paper])-set(assignment2.assignment_paper[reviewer_type][paper]))
                    if len(diff)>0:
                        self.loss_paper[paper] = diff

            reviewers = set(assignment1.assignment_reviewer[reviewer_type]) & set(assignment2.assignment_reviewer[reviewer_type])
            for reviewer in reviewers:
                if reviewer not in assignment2.assignment_reviewer[reviewer_type]:
                    self.gain_reviewer[reviewer_type][reviewer] = assignment1.assignment_reviewer[reviewer_type]
                elif reviewer not in assignment1.assignment_reviewer[reviewer_type]:
                    self.loss_reviewer[reviewer_type][reviewer] = assignment2.assignment_reviewer[reviewer_type]
                else:
                    diff = list(set(assignment2.assignment_reviewer[reviewer_type][reviewer]) - set(assignment1.assignment_reviewer[reviewer_type][reviewer]))
                    if len(diff)>0:
                        self.gain_reviewer[reviewer_type][reviewer] = diff
                    diff = list(set(assignment1.assignment_reviewer[reviewer_type][reviewer]) - set(assignment2.assignment_reviewer[reviewer_type][reviewer]))
                    if len(diff)>0:
                        self.loss_reviewer[reviewer_type][reviewer] = diff

    def prod(self, similarities, score_type=None):
        """Compute the similarity score change associated with an assignment difference."""
        score = 0.0
        if score_type is None:
            scs = similarities.scores
        elif score_type == 'tpms':
            scs = similarities.affinity
        elif score_type == 'subject':
            scs = similarities.subject_similarity
        for paper in self.loss_paper:
            for reviewer in self.loss_paper[paper]:
                if paper in scs.index and reviewer in scs.columns:
                    score -= scs.loc[paper, reviewer]
                else:
                    print("Warning paper", paper, "has no score for reviewer", reviewer)
        for paper in self.gain_paper:
            for reviewer in self.gain_paper[paper]:
                if paper in scs.index and reviewer in scs.columns:
                    score += scs.loc[paper, reviewer]
                else:
                    print("Warning paper", paper, "has no score for reviewer", reviewer)
        return score


class assignment:
    """
    Stores an assignment of reviewers to papers. The assignment can
    either be loaded (e.g. as an export from CMT) in or allocated
    using a similarities matrix.

    """
    def __init__(self, directory=None, max_reviewers=3, max_papers=4,  meta_reviewers_per_paper=1):

        if directory is None:
            directory = cmt_data_directory
        self.directory = directory
        self.quota = {}
        self.quota['reviewer'] = {}
        self.quota['metareviewer'] = {}
        self.max_reviewers = max_reviewers
        self.max_papers = max_papers
        self.meta_reviewers_per_paper = meta_reviewers_per_paper
        self.assignment_paper = {}
        self.assignment_reviewer = {}
        for type in ['reviewer', 'metareviewer']:
            self.assignment_paper[type] = {}
            self.assignment_reviewer[type] = {}

    def __minus__(self, other):
        """ Overloading of the '+' operator. for more control, see self.add """
        return self.diff(other)



    def reviewer_area_chairs(self, reviewer):
        """Return the area chairs responsible for managing a reviewer."""
        area_chairs = []
        for paper in self.assignment_reviewer['reviewer'][reviewer]:
            for chair in self.assignment_paper['metareviewer'][paper]:
                area_chairs.append(chair)
        return area_chairs

    def prod(self, similarities, score_type=None, reviewer_type='reviewer'):
        """Compute the similarity score of an assignment."""
        score = 0.0
        if score_type is None:
            scs = similarities.scores
        elif score_type == 'tpms':
            scs = similarities.affinity
        elif score_type == 'subject':
            scs = similarities.subject_similarity
        for paper in self.assignment_paper[reviewer_type]:
            for reviewer in self.assignment_paper[reviewer_type][paper]:
                if paper in scs.index and reviewer in scs.columns:
                    score += scs.loc[paper, reviewer]
                else:
                    print("Warning paper", paper, "has no score for reviewer", reviewer)
        return score

    def diff(self, other):
        """Compute the difference between two assignments for each paper and reviewer."""
        return assignment_diff(self, other)

    def load_assignment(self, filename=None, reviewer_type='reviewer'):
        """Load in the CMT assignments file."""
        self.clear_assignment(reviewer_type=reviewer_type)
        if filename==None:
            filename = 'Assignments.txt'
        if filename[-4:]=='.txt':
            with open(os.path.join(self.directory, filename)) as fin:
                rows = ( line.strip().split('\t') for line in fin)
                self.assignment_paper[reviewer_type] = {str(row[0]):[elem.lower() for elem in row[1:]] for row in rows}

            self._reviewer_from_paper(reviewer_type)
        elif filename[-4:] == '.xml':
            with open(os.path.join(self.directory, filename)) as xml_file:
                doc = etree.parse(xml_file)
            self.assignment_paper[reviewer_type] = {submission.get('submissionId'):[reviewer.get('email').lower() for reviewer in submission.xpath('./reviewer')] for submission in doc.xpath('/assignments/submission')}
            self._reviewer_from_paper(reviewer_type)

        elif filename[-4:] == '.xls':
            raise ValueError("un-implemented file type.")
        else:
            raise ValueError("unimplemented file type.")

    def _reviewer_from_paper(self, reviewer_type='reviewer'):
        """
        Set assignment_reviewer assuming assignment_paper is set correctly.
        """
        for paper in self.assignment_paper[reviewer_type]:
            for reviewer in self.assignment_paper[reviewer_type][paper]:
                if reviewer in self.assignment_reviewer[reviewer_type]:
                    self.assignment_reviewer[reviewer_type][reviewer].append(paper)
                else:
                    self.assignment_reviewer[reviewer_type][reviewer] = [paper]

    def update_group(self, group):
        """Update with a Data Series of true/false values which reviewers or area chairs are to be assigned."""
        self.group = group

    def prep_assignment(self):
        """Load quata and shotgun clusters in alongside conflicts in order to prepare for an assignment."""
        self.load_quota()
        print("Loaded Quota.")
        self.load_shotgun()
        print("Loaded shotgun clusters.")
        self.load_conflicts()
        print("Loaded Conflicts")

    def make_assignment(self, similarities, group=None, score_quantile=0.7, reviewer_type='reviewer'):
        if group is None:
            group = (similarities.reviewers.users['IsMetaReviewer']=='No') & (similarities.reviewers.users['IsReviewer']=='Yes')
        self.score_quantile = score_quantile
        self.prep_assignment()
        self.update_group(group)
        self.rank_similarity_scores(similarities)
        print("Ranked similarities")
        self.allocate(reviewer_type=reviewer_type)
        print("Performed allocation")

    def load_quota(self, filename='Reviewer Quotas.xls'):
        a = xl_read(filename=os.path.join(self.directory, filename), header=2, index_col='Reviewer Email', dataframe=True, lower_index=True)
        self.quota = a.items

    def unassigned_reviewers(self, reviewers, reviewer_type='reviewer', group=None):
        """Return a true/false series of reviewers that aren't at full allocation."""
        an = pd.Series(False, index=reviewers.users.index)
        for idx in an.index:
            if self.group.loc[idx]:
                if idx in self.assignment_reviewer[reviewer_type]:
                    num_assigned = len(self.assignment_reviewer[reviewer_type][idx])
                else:
                    num_assigned = 0
                if num_assigned<self.max_papers:
                    if idx not in list(self.quota.index):
                        an.loc[idx]=True
                    elif num_assigned<min([self.quota['Quota'][idx], self.max_papers]):
                        an.loc[idx] = True
                    else:
                        an.loc[idx] = False
                else:
                    an.loc[idx] = False
        return an

    def unassigned_papers(self, submissions, reviewer_type='reviewer'):
        """Return a true/false series of papers that are unassigned."""
        an = pd.Series(np.zeros(len(submissions.papers.index)), index=submissions.papers.index)
        for idx in an.index:
            #print idx
            if idx in self.assignment_paper[reviewer_type]:
                num_assigned = len(self.assignment_paper[reviewer_type][idx])
            else:
                num_assigned = 0
            if num_assigned<self.max_reviewers:
                an.loc[idx] = True
            else:
                an.loc[idx] = False
        return an

    def clear_assignment(self, reviewer_type='reviewer'):
        if reviewer_type is None:
            reviewer_types = ['reviewer', 'metareviewer']
        else:
            reviewer_types = [reviewer_type]
        for type in reviewer_types:
            self.assignment_paper[type] = {}
            self.assignment_reviewer[type] = {}

    def allocate(self,  reviewer_type='reviewer'):
        """Allocate papers to reviewers. This function goes through the similarities list *once* allocating papers. """

        for idx in list(self.score_vec.index):
            papers = str(self.score_vec['PaperID'][idx]).split('_')
            reviewer = str(self.score_vec['Email'][idx])
            assign = True
            for paper in papers:
                if not paper in self.assignment_paper[reviewer_type]:
                    self.assignment_paper[reviewer_type][paper] = []

                num_assigned = len(self.assignment_paper[reviewer_type][paper])+1
                if num_assigned>self.max_reviewers:
                    assign = False
                    continue
            if not assign:
                continue

            if not reviewer in self.assignment_reviewer[reviewer_type]:
                self.assignment_reviewer[reviewer_type][reviewer] = []
            num_assigned = len(self.assignment_reviewer[reviewer_type][reviewer]) + len(papers)
            if num_assigned>self.max_papers or (reviewer in list(self.quota.index) and num_assigned>self.quota['Quota'][reviewer]):
                continue

            # check paper isn't already assigned.
            for paper in papers:
                if paper in self.assignment_reviewer[reviewer_type][reviewer]:
                    assign = False
                    continue
            if not assign:
                continue

            for paper in papers:
                self.assignment_paper[reviewer_type][paper].append(reviewer)
            self.assignment_reviewer[reviewer_type][reviewer] += papers

    def load_shotgun(self, filename='ConstraintsGroup1.txt'):
        """
        Some papers have a very strong keyword clustering, and we'd like
        these to be reviewed alongside each other to check similarity

        """
        filename = 'ConstraintsGroup1.txt'
        with open(os.path.join(self.directory, filename)) as fin:
            rows = ( line.strip().split(' ') for line in fin)
            self.shotgun_clusters = [row for row in rows]
        fin.close()

    def load_conflicts(self, filename = 'Conflicts.txt'):
        """Load in the CMT conflicts file."""
        with open(os.path.join(self.directory, filename)) as fin:
            rows = ( line.strip().split('\t') for line in fin)
            self.conflicts_groups = {str(row[0]):[elem.lower() for elem in row[1:]] for row in rows}
        self.conflicts_by_reviewer = {}

        for paper in self.conflicts_groups:
            for reviewer in self.conflicts_groups[paper]:
                if reviewer in self.conflicts_by_reviewer:
                    self.conflicts_by_reviewer[reviewer].append(paper)
                else:
                    self.conflicts_by_reviewer[reviewer] = [paper]


    def rank_similarity_scores(self, similarities):
        """
        Place the similarity scores into a 'melted' structure and rank so that highest similarity is top.
        """
        # Allocate 'expert reviewers' those with 2 or more papers.
        rank_scores = similarities.scores.copy()
        # Normalise
        rank_scores = rank_scores/rank_scores.std()
        rank_scores = (rank_scores.T/rank_scores.T.std()).T

        for paper in self.conflicts_groups:
            rank_scores.loc[paper][self.conflicts_groups[paper]] = -np.inf

        # select users to allocate
        usergroup = similarities.reviewers.users[self.group].index
        rank_scores=rank_scores[usergroup]

        # merge shotgun papers for ranking.
        for cluster in self.shotgun_clusters:
            cluster_name = '_'.join(cluster)
            rank_scores.loc[cluster_name] = rank_scores.loc[cluster[0]]
            for paper in cluster[1:]:
                rank_scores.loc[cluster_name] += rank_scores.loc[paper]
            rank_scores.loc[cluster_name]/=len(cluster)
            rank_scores.drop(cluster, inplace=True)

        print("Allocating to", len(usergroup), "users.")
        self.score_vec = rank_scores.reset_index()
        self.score_vec = pd.melt(self.score_vec, id_vars=['index']) # Opposite of a pivot!
        val = self.score_vec.value.quantile(self.score_quantile)
        print("Retaining scores greater than", self.score_quantile*100, "percentile which is", val)
        self.score_vec = self.score_vec[self.score_vec.value >val]
        self.score_vec = self.score_vec[pd.notnull(self.score_vec.value)]
        self.score_vec.columns = ['PaperID', 'Email', 'Score']
        self.score_vec = self.score_vec.sort_index(by='Score', ascending=False)
        self.score_vec.reset_index(inplace=True)

    def _repr_html_(self):
        """Print an html representation of the assignment."""
        html = '<table>'
        html+= '<tr><td>Paper</td><td>Area Chair</td><td>Reviewers</td></tr>\n'
        for paper in list(set(self.assignment_paper['reviewer'])
                          | set(self.assignment_paper['metareviewer'])):
            html += '<tr><td>' + paper + '</td>'
            if paper in self.assignment_paper['metareviewer']:
                html += '<td>' + ','.join(self.assignment_paper['metareviewer'][paper]) + '</td>'
            else:
                html += '<td></td>'
            if paper in self.assignment_paper['reviewer']:
                html += '<td>' + ','.join(self.assignment_paper['reviewer'][paper]) + '</td></tr>\n'
            else:
                html += '<td></td>'
        html += '</table>'
        return html

    def write(self, reviewer_type='reviewer'):
        """Write out the assignment into an xml file for import into CMT."""
        f = open(os.path.join(self.directory, reviewer_type + '_assignments.xml'), 'w')
        f.write('<assignments>\n')
        for paper in self.assignment_paper[reviewer_type]:
            f.write('  <submission submissionId="' + paper + '">\n')
            for reviewer in self.assignment_paper[reviewer_type][paper]:
                f.write('    <' + reviewer_type + ' email="' + reviewer + '"/>\n')
            f.write('  </submission>\n')
        f.write('</assignments>\n')
        f.close()

class tpms:
    """
    """
    def __init__(self, filename='cmt_export.txt'):
        """Download the status of the TPMS system from Laurent's output script and update reviewers in the system who's TPMS status is unavailable."""
        # Get Laurent's latest list (this is updated hourly).
        url = 'http://papermatching.cs.toronto.edu/paper_collection/cmt_export_nips14.txt'
        writename = os.path.join(cmt_data_directory, filename)
        #download_url(url, writename)
        self.reviewers = []
        import csv
        with open(writename, 'rb') as csvfile:
            file_reader = csv.reader(csvfile, delimiter=',')
            for row in file_reader:
                if len(row)>3:
                    self.add_reviewer(row)
        self.reviewers = pd.DataFrame(self.reviewers)

    def add_reviewer(self, reviewer):
        """Add reviewer info from TPMS"""
        firstName = reviewer[0].title()
        lastName = reviewer[1].title()
        review_dict = {'FirstName':firstName,
                       'LastName':lastName,
                       'MiddleNames':'',
                       'Email':reviewer[2],
                       'TPMSstatus':None,
                       'TPMSemail':''}

        if reviewer[3].strip() == 'unavailable':
            review_dict['TPMSstatus'] = 'unavailable'
            if reviewer[4].strip() == 'exact match':
                # extract the CMT email here
                review_dict['TPMSemail'] = review_dict['Email']
                email = reviewer[5].replace("alternates['" + review_dict['Email'] + "'] = ", "")
                email = email.replace("'", "")
                review_dict['Email'] = email
                self.reviewers.append(review_dict)
            elif reviewer[4].strip() == 'partial match':
                print("Partial match with ", ', '.join(reviewer))
            else:
                self.reviewers.append(review_dict)
        elif reviewer[3].strip() == 'available but no pdfs':
            review_dict['TPMSstatus'] = 'nopdfs'
            self.reviewers.append(review_dict)
        elif reviewer[3].strip()[:26] == 'user profile automatically':
            review_dict['TPMSstatus'] = 'auto'
            self.reviewers.append(review_dict)
        else:
            raise ValueError('Unknown reviewer type' + ', '.join(reviewer))

class ReadReviewer:
    def __init__(self, filename):
        self.filename = filename

class pc_groupings():
    """This class handles the storage and processing of program committee groupings, between buddy pairs, or teleconference groups and the like. Groupings are read from a google document with columns that contain 1) an index to the group [index], 2) the name of the group [group], 3) the program chair responsible for the group [chair], 4) the email address of the the area chair in CMT [email], 5) optionally the gmail address to use for spreadsheet sharing etc. [gmail]"""
    def __init__(self, resource_id, conflicts_file, assignment_file, worksheet_name='Sheet1'):
        self.create_spreadsheet = True
        self.resource_ids = {}
        self.assignment = assignment()
        self.assignment.load_assignment(filename=assignment_file, reviewer_type='metareviewer')
        resource_ids_file = os.path.join(cmt_data_directory, resource_id + '.pickle')
        self.docs_client = None
        self.gd_client = None
        #if these groups have been set up already, the pickle file of the spreadsheet keys should exist.
        if os.path.isfile(resource_ids_file):
            self.create_spreadsheet = False
            self.resource_ids = pickle.load(open(resource_ids_file, 'rb'))

        bp = pods.google.sheet(resource_id=resource_id, worksheet_name=worksheet_name)
        self.groups = bp.read()
        with open(os.path.join(cmt_data_directory, conflicts_file)) as fin:
            rows = ( line.strip().split('\t') for line in fin)
            conflicts_groups = { row[0]:row[1:] for row in rows}
        papers = conflicts_groups.keys()
        self.conflicts_by_area_chair = {}
        self.conflicts_dict = {}
        for paper in papers:
            for area_chair in conflicts_groups[paper]:
                if self.conflicts_by_area_chair.has_key(area_chair):
                    self.conflicts_by_area_chair[area_chair].append(paper)
                else:
                    self.conflicts_by_area_chair[area_chair] = [paper]
        # Fields to extract from area chair's spreadsheets, rather than from the CMT derived reports.
        self.update_field_list = ['notes', 'accept', 'talk', 'spotlight']

        self.update_papers()
        all_papers = []
        for  papers in self._papers.values():
            all_papers += papers
        self.report = pd.DataFrame(index=all_papers)

        comment="""Click Me for Notes!
            Based on processed reviews form 2014/8/12.
            This report gives the status of the papers that don't conflict within your buddy-group.
            Please use it to identify papers where there may be ongoing problems.
            Look out for papers with a high attention score and little or no discussion.
            Your notes can be placed in the 'note' column.
            Tentative accept/talk/spotlight decisions can be made by placing a 'y' for yes or 'm' for maybe in the relevant column."""
        comment_conflicted="""These are papers that conflict with your buddy group, they will need to be dealt with separately.
            Based on processed reviews form 2014/8/12."""

    def update_report(self, groups=None):
        """
        Update the report with information from the spreadsheets.
        """
        if groups is None:
            groups = self.resource_ids.keys()
        for group in groups:
            data_frame = self.data_from_spreadsheet(group)
            self._to_report(group, data_frame)

    def update_papers(self):
        """Update the the lists of papers in the groups. """

        self._papers = {}
        for group in sorted(set(self.groups.index), key=int):
            group_name = self.groups.loc[group].group[0]
            self._papers[group_name] = []
            group_df = self.groups.loc[group]
            for index, area_chair in group_df.iterrows():
                conflict_papers = []
                for chair in group_df['email']:
                    conflict_papers += self.conflicts_by_area_chair[chair]
                    self.conflicts_dict[chair] = []
                for paper in self.assignment.assignment_reviewer['metareviewer'][area_chair['email']]:
                    if paper in conflict_papers:
                        self.conflicts_dict[chair].append(paper)
                    else:
                        self._papers[group_name].append(paper)
            for index, area_chair in group_df.iterrows():
                email = area_chair['email']
                self._papers[email]=list(set(self.assignment.assignment_reviewer['metareviewer'][email]) - set(self._papers[group_name]))

    def _to_report(self, group, data_frame):
        """Update the report with information pulled from the area chair."""
        papers = self._papers[group]
        for paper in papers:
            if paper not in data_frame.index:
                raise ValueError("Paper " + str(paper) + " not present in spreadsheet obtained from group.")
        for paper in data_frame.index:
            if paper not in papers:
                print("Paper ", paper, " appears to have been withdrawn (it is not in the Attention Report).")
        self.report = data_frame.loc[papers].combine_first(self.report)

    def data_from_spreadsheet(self, group):
        """
        Extract the data from one of the group's spreadsheets, return a
        data frame containing the information.
        """
        ss = pods.google.sheet(resource=resource(id=self.resource_ids[group]), gd_client=self.gd_client, docs_client=self.docs_client)
        self.gd_client = ss.gd_client
        self.docs_client = ss.docs_client
        return ss.read(header_rows=2)

    def data_to_spreadsheet(self, group, data_frame, comment=''):
        """Update the spreadsheet with the latest version of the group report."""
        ss = pods.google.sheet(resource=self.resource[group], gd_client=self.gd_client, docs_client=self.docs_client,)
        self.gd_client = ss.gd_client
        self.docs_client = ss.docs_client
        ss.write(data_frame, comment=comment, header_rows=2)


class drive_store(pods.google.sheet, ReadReviewer):
    def __init__(self, resource, worksheet_name):
        pods.google.sheet.__init__(self, resource=resource, worksheet_name=worksheet_name)

    def read(self, column_fields=None, header_rows=1, index_field='Email'):
        """Read potential reviewer entries from a google doc."""
        entries = pods.google.sheet.read(self, column_fields, header_rows)

        # do some specific post-processing on columns
        if 'ScholarID' in entries.columns:
            entries['ScholarID'].apply(lambda value: re.sub('&.*', '', re.sub('.*user=', '', value.strip())) if not pd.isnull(value) else '')
        if 'Email' in entries.columns:
            entries['Email'].apply(lambda value: value.strip().lower() if not pd.isnull(value) else '')
        if 'Name' in entries.columns:
            entries['FirstName'], entries['MiddleNames'], entries['LastName']  = split_names(entries['Name'])
        self.reviewers=entries

    def read_meta_reviewers(self):
        column_fields={'1':'Name', '2':'Institute', '3':'Subjects', '4':'Email', '5':'Answer'}
        self.read(column_fields)


    def read_reviewer_suggestions(self):
        """Read in reviewer suggestions as given by area chairs through the reviewer suggestion form."""
        column_fields={'1':'TimeStamp', '2':'FirstName', '3':'LastName', '4':'MiddleNames', '5':'Email', '6':'Institute', '7':'Nominator', '9':'ScholarID'}
        self.read(column_fields)

    def read_nips_reviewer_suggestions(self):
        """Read in reviewer suggestions from lists of people who've had NIPS papers since a given year."""
        yearkey = 'PapersSince' + year
        column_fields={'1':'FirstName', '2':'MiddleNames', '3':'LastName', '4':'Email', '5':'Institute', '6':'ScholarID', '7':yearkey, '8':'decision'}
        self.read(column_fields)



class area_chair_read:
  """
  This class reads area chairs from previous conferences
  """
  def __init__(self, conference, year):
      if conference == 'nips':
          conference = 'ac'
      self.filename = conference + str(year) + '.txt'
      fname = os.path.join(nips_data_directory, 'conference-committees', self.filename)
      self.chairs = []
      import csv
      with open(fname, 'rb') as csvfile:
          reader = csv.reader(csvfile, delimiter='\t')
          for line in reader:
              chair = {}
              chair['FirstName'], chair['MiddleNames'], chair['LastName'] = split_names(line[0])
              if len(line)>1:
                  chair['Institute'] = line[1]
              else:
                  chair['Institute'] = ''
              if len(line)>2:
                  chair['SubjectString'] = line[2]
              else:
                  chair['SubjectString'] = ''
              self.chairs.append(chair)

# legacy code used in Update with NIPS Paper Publications.ipynb
class old_csv_read(ReadReviewer):
    def __init__(self, filename='users.csv', header_row=1):
        self.filename = filename
        self.reviewers = []
        fname = os.path.join(cmt_data_directory, self.filename)
        mapping = {'NumPapers' : 'PapersSince2007'}
        import csv
        row_count = 0
        field = []
        with open(fname, 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')

            for row in reader:
                reviewer = {}
                row_count+=1
                if row_count==header_row:
                    for entry in row:
                        field.append(entry)
                elif row_count > header_row:
                    for i, entry in enumerate(row):
                        reviewer[field[i]] = entry
                    self.reviewers.append(reviewer)

class csv_read:
    """
    Read a data frame from a csv file in a similar format as xl_read to allow csv and xls to be loaded interchangeably.
    """
    def __init__(self, filename='file.csv', header=0, mapping=None, index_col=None, lower_index=False, ignore = [], parse_dates = []):
        self.items = pd.read_csv(filename, header=header, parse_dates=parse_dates)
        self.items.rename(columns=mapping, inplace=True)
        self.items.set_index(index_col, inplace=True)
        self.filename = filename

class xl_read:
    """
    Read a data frame from an excel file in the form CMT exports (which is XML derived).
    """
    def __init__(self, filename='file.xls', header=0, mapping=None, index_col=None, dataframe=False, worksheet_number=0, lower_index=False, ignore = [], parse_dates=[]):
        heading_row = header+1
        self.filename = filename
        fname = os.path.join(cmt_data_directory, self.filename)
        self.column = {}
        items = []

        with open(fname) as xml_file:
            doc = etree.parse(xml_file)

        namespaces={'o':'urn:schemas-microsoft-com:office:office',
                    'x':'urn:schemas-microsoft-com:office:excel',
                    'ss':'urn:schemas-microsoft-com:office:spreadsheet'}

        ws = doc.xpath('/ss:Workbook/ss:Worksheet', namespaces=namespaces)
        if len(ws) > 0:
            if not worksheet_number<len(ws):
                raise "Error worksheet number does not exist."
            tables = ws[worksheet_number].xpath('./ss:Table', namespaces=namespaces)
            if len(tables) > 0:
                rows = tables[0].xpath('./ss:Row', namespaces=namespaces)
                row_count = 0
                for row in rows:
                    ind = row.get('{%(ss)s}Index' % namespaces);
                    if ind is None:
                        row_count += 1
                    else:
                        row_count = int(ind)
                    cells = row.xpath('./ss:Cell/ss:Data', namespaces=namespaces)
                    col_count = 0
                    item = {}
                    for cell in cells:
                        ind = cell.get('{%(ss)s}Index' % namespaces);
                        if ind is None:
                            col_count += 1
                        else:
                            col_count = int(ind)
                        if row_count==heading_row:
                            if mapping and cell.text in mapping.keys():
                                text = mapping[cell.text]
                            else:
                                text = cell.text
                            self.column[str(col_count)] = text


                        elif row_count>heading_row:
                            col = self.column[str(col_count)]
                            if col in ignore:
                                continue
                            try:
                                val = float(cell.text)
                                if val - int(val) == 0.0:
                                    val = int(cell.text)
                                item[col] = val
                            except (ValueError, TypeError):
                                item[col] = cell.text

                            if dataframe:
                                if not index_col:
                                    raise ValueError("Data frame needs an index.")
                                if col==index_col:
                                    if lower_index:
                                        item[col] = cell.text.lower()
                                    else:
                                        item[col] = cell.text

                    if row_count > heading_row:
                        if dataframe:
                            if not index_col:
                                raise ValueError("Data frame needs an index column.")
                            if not index_col in item.keys():
                                raise ValueError("Data has no column " + index_col + " for index.")
                            index_val = item[index_col]
                            del item[index_col]

                            items.append(pd.DataFrame(item, index=[index_val]))
                        else:
                            items.append(item)
        if dataframe:
            self.items = pd.concat(items)
            self.items.index.name = index_col
            if parse_dates and len(parse_dates)>0:
                for column in parse_dates:
                    self.items[column] = pd.to_datetime(self.items[column])
        else:
            self.items = items
# Read CMT Reviews
class cmt_reviews_read:
    """
    Read an export of the reviews from CMT.
    """
    def __init__(self, filename='Reviews.xls', header=2, dataframe=True):
        ignore = ['Confidence', 'Impact Score - Independently of the Quality Score above, this is your opportunity to identify papers that are very different, original, or otherwise potentially impactful for the NIPS community.','Quality Score - Does the paper deserves to be published?', 'Rank', 'RankComment']
        mapping = {'SubmissionId': 'ID',
                   'SubmissionName' : 'Title',
                   'Comments to author(s).  First provide a summary of the paper, and then address the following criteria:  Quality, clarity, originality and significance.   (For detailed reviewing guidelines, see http://nips.cc/PaperInformation/ReviewerInstructions)' : 'Comments',
                   'Quality Score - Does the paper deserves to be published? (Numeric)' : 'Quality',
                   'Impact Score - Independently of the Quality Score above, this is your opportunity to identify papers that are very different, original, or otherwise potentially impactful for the NIPS community. (Numeric)' : 'Impact',
                   'Confidence (Numeric)' : 'Conf',
                   'Confidential comments to the PC members' : 'Confidential',
                   'Please summarize your review in 1-2 sentences' : 'Summary'}

        data = read_xl_or_csv(filename, header=header, mapping=mapping, index_col='ID', dataframe=dataframe, parse_dates=['LastUpdated'])
        self.reviews = data.items

# Read CMT Papers
class cmt_papers_read:
    """
    Read list of papers exported from CMT under the 'decision' column.
    """
    def __init__(self, filename='Papers.xls', header=2, dataframe=True):
        mapping = {'Paper ID': 'ID',
                   'Paper Title' : 'Title',
                   'Abstract': 'Abstract',
                   'Author Names': 'AuthorNames',
                   'Author Emails': 'AuthorEmails',
                   'Subject Areas': 'SubjectAreas',
                   'Conflict Reasons': 'ConflictReasons',
                   'Files': 'Files',
                   'Supplemental File': 'SupplementalFile',
                   'Dual Submission Policy': 'DualSubmissionPolicy',
                   'Dual Submissions': 'DualSubmissions'}
        data = read_xl_or_csv(filename, header, mapping, index_col='ID', dataframe=dataframe)
        self.papers = data.items

# Read CMT metareviews
class cmt_metareviews_read(xl_read):
    """
    Read the metareviews from CMT for analysis.
    """
    def __init__(self, filename='Reviews.xls', header_row=3, dataframe=True):
        ignore = ['LastUpdated', 'Organization', 'Rank', 'RankComment', 'Overall Rating']
        mapping = {'SubmissionId': 'ID',
                   'SubmissionName' : 'Title',
                   'FirstName' : 'FirstName',
                   'LastName' : 'LastName',
                   'Email' : 'Email',
                   'Overall Rating (Numeric)' : 'Rating',
                   'Detailed Comments' : 'Comments'}
        xl_read.__init__(self, filename, header_row, mapping, index='ID', dataframe=dataframe, ignore=ignore)
        self.reviews = self.items

# Read CMT Author Feedback Status
class cmt_authorfeedback_read(xl_read):
    """
    Read Author Feedback Status for analysis.
    """
    def __init__(self, filename='Papers.xls', header_row=3, dataframe=True):
        mapping = {'Paper ID': 'ID',
                   'Author Feedback Submitted': 'feedbackStatus'}
        xl_read.__init__(self, filename, header_row, mapping, index='ID', dataframe=dataframe)
        self.papers = self.items

def read_xl_or_csv(filename, header, mapping, index_col, dataframe, parse_dates=None):
    """Helper function for switching between xls and csv reads."""
    _, ext = os.path.splitext(filename)
    if ext == '.xls':
        return xl_read(filename, header, mapping, index_col=index_col, dataframe=dataframe, parse_dates=parse_dates)
    elif ext == '.csv':
        return csv_read(filename, header, mapping, index_col=index_col, parse_dates=parse_dates)
    else:
        raise ValueError("Unknown file extension: " + ext)

class cmt_reviewers_read:
    """
    Read information from a CMT export file into the standard Reviewers
    format.
    """
    def __init__(self, filename='Conflict Domains.xls', header=2, dataframe=False):
        mapping = {'MiddleInitial': 'MiddleNames',
                   'Organization' : 'Institute',
                   'Last Name': 'LastName',
                   'First Name': 'FirstName',
                   'Reviewer Type': 'ReviewerType'}
        data = read_xl_or_csv(filename, header, mapping, index_col, dataframe)
        self.reviewers = data.items



class reviewerdb:
    def __init__(self, filename):
        self.filename=filename
        self.dbfile = os.path.join(cmt_data_directory,self.filename)

    def _repr_html_(self):
        """Create an HTML representation of the database for display in the notebook"""
        return self.to_data_frame()._repr_html_()
    def _add_keys_if_present(self, id, reviewer, keys):
        for key in keys:
            if key in reviewer.keys():
                if reviewer[key]:
                    a = self.update_field(id, key, reviewer[key])
                    print("Updated ", key, " for ID ", reviewer['FirstName'], reviewer['LastName'], " as ", reviewer[key])

    def to_data_frame(self):
        """Returns the data base as a pandas data frame."""
        conn = sqlite3.connect(os.path.join(cmt_data_directory,self.filename))
        return pd.read_sql("SELECT * from Reviewers", conn)

    def add_users(self, reviewers, fieldname=None, yes=False, query=True, match_firstname=False, match_lastname=True):
        count = 0
        for i, reviewer in reviewers.iterrows():
            print("Processed ", count, " out of ", len(reviewers), " reviewers.")
            id = self.match_or_add_reviewer(reviewer, yes=yes, query=query, match_firstname=match_firstname, match_lastname=match_lastname, fieldname=fieldname)

            if id:
                self._add_keys_if_present(id, reviewer, ['ScholarID', 'Nominator', 'PapersSince2007'])
                if 'Answer' in reviewer:
                    if reviewer['Answer'] == 'Y':
                        self._execute_sql("UPDATE Reviewers SET MetaReviewer=1 WHERE ID=" + str(id) + ";", commit=True)
                    elif reviewer['Answer'] == 'N':
                        self._execute_sql("UPDATE Reviewers SET MetaReviewer=0 WHERE ID=" + str(id) + ";", commit=True)
                else:
                    print("No recorded answer from", reviewer['FirstName'], reviewer['LastName'])
            else:
                print('Skipping add for', reviewer['Email'], "from reviewer", reviewer['FirstName'], reviewer['MiddleNames'], reviewer['LastName'], "of", reviewer['Institute'])
            count += 1

    def create_reviewer_table(self):
        conn = sqlite3.connect(self.dbfile)
        conn.execute('''CREATE TABLE IF NOT EXISTS Reviewers
            (ID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
            FirstName TEXT NOT NULL,
            MiddleNames TEXT,
            LastName TEXT NOT NULL,
            Institute TEXT NOT NULL,
            Email TEXT NOT NULL,
            Subjects TEXT,
            Conflicts TEXT,
            Webpage TEXT,
            MSAcademicID INTEGER,
            ScholarID TEXT,
            ResearchGateID TEXT,
            Active INTEGER
            SecondRoundInvite INTEGER,
            FirstRoundInvite INTEGER,
            NipsDatabaseNominated INTEGER,
            Nominator TEXT,
            PapersSince2007 INTEGER,
            MetaReviewer INTEGER);''')
        conn.commit()
        conn.close()

    def create_conference_table(self, conference, year):
        tablename = conference.upper() + str(year)
        conn = sqlite3.connect(self.dbfile)
        conn.execute("CREATE TABLE IF NOT EXISTS " + tablename + " (ID INTEGER PRIMARY KEY NOT NULL, SubjectString TEXT NOT NULL);")
        conn.close()

    def was_area_chair(self, ID, conference, year):
        tablename = conference.upper() + str(year)
        return self._execute_sql("""SELECT COUNT(ID) FROM """ + tablename + """ WHERE ID=""" + str(ID) + """;""")[0][0]>0


    def _execute_sql(self, command, commit=False):
        # execute an sql command on the data base.
        conn = sqlite3.connect(self.dbfile)
        cur = conn.execute(command)
        if commit:
            conn.commit()
            ans = conn.total_changes
        else:
            ans = cur.fetchall()
        conn.close()
        return ans

    def _string_sql(self, command, commit=False):
        # print results of an sql command on the data base.
        ans = self._execute_sql(command, commit)
        return self._table_to_string(ans)

    def _table_to_string(self, table):
        # Simple formatting of a table for printing.
        string = u''
        for row in table:
            for col in row:
                string += unicode(col) + '\t'
            string+='\n'
        return string

    def set_reviewer(self, id):
        """Set whether or not the user is a reviewer."""
        self.update_field(id, 'IsReviewer', 1)

    def get_field(self, id, fieldname=None):
        val = None
        if fieldname and id:
            val = self._execute_sql("SELECT " + fieldname + " FROM Reviewers WHERE ID=" + str(id) + ";")
        return val[0][0]

    def augment_emails(self, id, email, replace=True):
        """
        Replace primary email with that given and move current primary to
        othermails list.
        """
        othermails = self.get_field(id, 'OtherEmails')
        if not othermails:
            othermails = ''
        if replace:
            oldmail = self.get_field(id, 'Email')
            replacemail = email.lower()
        else:
            oldmail = email.lower()
            replacemail = self.get_field(id, 'Email')

        if oldmail not in othermails.split(';'):
            if len(othermails)>0:
                othermails += ';'
            othermails += oldmail

        if replacemail in othermails:
            othermails_list = othermails.split(';')
            if replacemail in othermails_list:
                othermails_list.remove(replacemail)
            othermails = ';'.join(othermails_list)

        self.update_field(id, 'OtherEmails', othermails.lower())
        self.update_field(id, 'Email', replacemail)

    def update_field(self, id, fieldname=None, fieldvalue=None, commit=True):
        """
        Update a field in the reviewer data base.
        """
        ans = None
        if fieldname:
            if not fieldvalue:
                fieldvalue = 'NULL'
            elif isinstance(fieldvalue, float) and np.isnan(fieldvalue):
                fieldvalue = 'NULL'
            elif isinstance(fieldvalue, str) or isinstance(fieldvalue, unicode):
                fieldvalue = "'" + fieldvalue.strip().replace("'", "''") + "'"
            else:
                fieldvalue = str(fieldvalue)
            if not fieldname=='IsReviewer' and not fieldname=='IsMetaReviewer':
                print("Updating ", fieldname, " to ", fieldvalue)
            a = self._execute_sql("UPDATE Reviewers SET " + fieldname + "=" + fieldvalue + " WHERE ID=" + str(id) + ";", commit=True)
        return ans

    def update_fields(self, idslist, fieldname=None, fieldvalue=None):
        """Update a field in the reviewer data base for a list of IDs"""
        id_str = ''
        empty = True
        for ids in idslist:
            for id in ids:
                if not empty:
                    id_str += ', '
                else:
                    empty = False
                id_str += str(id)
        ans = None
        if fieldname:
            if not fieldvalue:
                fieldvalue = 'NULL'
            elif isinstance(fieldvalue, str) or isinstance(fieldvalue, unicode):
                fieldvalue = "'" + fieldvalue.strip().replace("'", "''") + "'"
            else:
                fieldvalue = str(fieldvalue)
            if not fieldname=='IsReviewer' and not fieldname=='IsMetaReviewer':
                print("Updating ", fieldname, " to ", fieldvalue)
            ans = self._execute_sql("UPDATE Reviewers SET " + fieldname + "=" + fieldvalue + " WHERE ID IN (" + id_str + ");", commit=True)
        return ans

    def update_reviewer(self, reviewer, yes=False, query=True, match_firstname=False, match_lastname=True, fields=None):
        """
        Take a reviewer information from (for example CMT) and update the
        reviewer in the data base. Set the fieldname in the data base to
        be true for the reviewer.

        """
        id = self.match_reviewer(reviewer, yes=yes, query=query, match_firstname=match_firstname, match_lastname=match_lastname)
        if id:
            for field, value in fields.iteritems():
                self.update_field(id, field, value)

    def match_tpms_status(self, tpms, status='unavailable'):
        """
        Find the ids of all reviewers who's TPMS in the passed class
        status is as given in status.

        """
        idlist = []
        for index, reviewer in tpms.reviewers.iterrows():
            if reviewer['TPMSstatus']==status:
                email = reviewer['Email'].strip()
                email = email.lower()
                ids = self.all_email_id(email)
                if len(ids) ==0:
                    print("No match for reviewer with email: ", email)
                elif len(ids) > 1:
                    print("Multiple matches for reviewer with email: ", email)
                else:
                    idlist.append(ids[0][0])
        return idlist

    def match_reviewer(self, reviewer, yes=False, query=True, match_firstname=False, match_lastname=True, primary_email=False):
          id = None
          ids = self.email_id(email=reviewer['Email'])
          num_matches = len(ids)
          if num_matches==1:
              return ids[0][0]
          ids = self.all_email_id(email=reviewer['Email'])
          if len(ids)==1:
              self.augment_emails(ids[0][0], reviewer['Email'], primary_email)
              return ids[0][0]
          elif num_matches>1:
              id=self._select_match(ids, reviewer)
              self.augment_emails(id, reviewer['Email'], primary_email)
              return id

          # Try to match first name and last name
          print("No email match.")
          print("Attempting lastname/firstname match for ", reviewer['FirstName'], reviewer['LastName'], "with email", reviewer['Email'])
          ids = self.firstname_lastname_id(reviewer['FirstName'], reviewer['LastName'])
          num_matches = len(ids)
          if num_matches==1:
              id = ids[0][0]
              match = True
              print("Lastname/firstname match succesful. Substituting primary email.")
              proceed = self._query_user('Proceed? (Y):', 'Y', query)
              if not proceed == 'Y':
                  print("User not modified.")
                  id = None
              else:
                  self.augment_emails(id, reviewer['Email'], primary_email)
                  return id


          if num_matches>1:
              id=self._select_match(ids, reviewer)
              if id:
                  self.augment_emails(id, reviewer['Email'], primary_email)
              else:
                  id = None

          if num_matches==0:
              ids = []
              print("Lastname/firstname match failed")
              if match_firstname:
                  print("Finding matching first names.")
                  ids = self.firstname_id(reviewer['FirstName'])
              if match_lastname:
                  print("Finding matching last names.")
                  ids += self.lastname_id(reviewer['LastName'])
              if len(ids)>0:
                  print("Requesting User input for Name Match.")
                  id = self._select_match(ids, reviewer)
                  if id:
                      self.augment_emails(id, reviewer['Email'], primary_email)
          if not id:
              print("Warning match has failed.")
          return id

    def match_or_add_reviewer(self, reviewer, yes=False, query=True, match_firstname=False, match_lastname=True, fieldname=None, primary_email=False):
      """
      Try to see if reviewer is in data base, if there is no match on
      first or last name then attempt to add reviewer.

      """

      id = self.match_reviewer(reviewer, yes=yes, query=query, match_firstname=match_firstname, match_lastname=match_lastname, primary_email=primary_email)
      if not id:
          id = self._request_new_reviewer(reviewer, yes=yes, query=query)
          if id:
              a = self.update_field(id, fieldname, 1)
      return id


    def add_chair_information(self, conference, year):
        """Add information from a list of area chairs for a particular conference to the data base."""
        conf = area_chair_read(conference, year)
        for chair in conf.chairs:
            id = self.match_or_add_reviewer(reviewer=chair)
            if id:
                self.add_chaired_conference(ID=id, conference=conference, year=year, reviewer=chair)


    def _select_match(self, ids, reviewer):
        """
        Present a list of potential matches and their ids for selection. If a negative response is given return none.
        """
        print_string = ''
        for id in ids:
            print_string += (self._string_sql("SELECT ID, FirstName, LastName FROM Reviewers WHERE ID=" + str(id[0]))).strip()
            print_string += '\n'
        print(print_string)
        ans = raw_input(reviewer['FirstName'] + ' ' + reviewer['LastName'] +  " add to a given ID. Reply N to add new user? N")
        if ans == 'N' or ans=='n' or ans=='':
            return None
        else:
            if ans.isdigit():
                print("Selected reviewer ", int(ans))
                return int(ans)
            else:
                print("Number required or N/n required.")
                return self._select_match(ids, reviewer)

    def _request_new_reviewer(self, reviewer, yes=False, query=True):
        """
        Get information about a new data base entry from the reviewer.

        """

        if not yes:
            print("Add new reviewer to reviewer data base?")
        else:
            print("Adding new reviewer to reviewer data base!")

        institute=''
        if 'Institute' in reviewer and reviewer['Institute']:
            institute = 'of ' + reviewer['Institute']
        print("Current info is: ", reviewer['FirstName'], reviewer['LastName'], "with email", reviewer['Email'],  institute)
        print("Google Search:")
        print()

        url = u"https://www.google.co.uk/search?q="+reviewer['FirstName'].strip()+u"+"+reviewer['LastName'].strip()
        display_url(url)
        print()
        if not yes:
            proceed = self._query_user('Proceed? (Y):', 'Y', query)
            if not proceed == 'Y':
                print("Not adding user.")
                return None
        for key in reviewer.keys():
            reviewer[key] = self._query_user(key + ':', reviewer[key], query)

        if not yes:
            print("Add reviewer ", reviewer['FirstName'], reviewer['MiddleNames'], reviewer['LastName'], "of", reviewer['Institute'], "with email", reviewer['Email'], "?")
            ans = raw_input("(Y/N): N?")
            if not ans=='Y' and not ans=='y':
                return self._request_new_reviewer(reviewer)
        self.add_reviewers([reviewer], check_email=True)
        return self.email_id(reviewer['Email'])[0][0]

    def _query_user(self, prompt, variable, query=True):
        if not query:
            return variable
        if variable=='' or variable==None:
            return raw_input(prompt)
        else:
            ans = raw_input(prompt + '(default: ' + unicode(variable) + ')')
            if not ans == '':
                return ans
        return variable

    def add_chaired_conference(self, ID, conference, year, subject='',reviewer=None):
        self.create_conference_table(conference, year)
        if ID==None and reviewer is not None:
            self.add_reviewers([reviewer])
            id = self.all_email_id(reviewer['Email'])
            if subject=='' and 'SubjectString' in reviewer:
                subject = reviewer['SubjectString']
        else:
            id = ID
            if not reviewer==None and subject=='':
                if 'SubjectString' in reviewer:
                    subject = reviewer['SubjectString']
        if self.add_chair(id, conference, year, subject=subject):
            if ID:
                print("Added Reviewer ID ", ID, " to ", conference, year)
            else:
                print("Added ", reviewer['Email'], " to ", conference, year)
        else:
            print("Reviewer", id, "already in area chair list.")

    def add_chair(self, ID, conference, year, subject):
        """Add an area chair to a conference table."""
        tablename = conference.upper() + str(year)
        ids = self._execute_sql("SELECT ID FROM " + tablename + " WHERE ID=" + str(ID))
        if not ids:
            self._execute_sql("INSERT INTO " + tablename + " (ID, SubjectString) VALUES (" + str(ID) + ", \"" + subject + "\")", commit=True)
            return True
        else:
            return False

    def add_reviewers(self, reviewers, check_email=True):
        """Add reviewers to the data base of reveiwers."""
        conn = sqlite3.connect(os.path.join(cmt_data_directory,self.filename))
        for reviewer in reviewers:

            if 'FirstName' in reviewer:
                Firstname = reviewer['FirstName']
            else:
                Firstname = ''
                if Firstname is None:
                    raise "Reviewer needs a first name."
            if 'LastName' in reviewer:
                Lastname = reviewer['LastName']
            else:
                Lastname = ''
                if Lastname is None:
                    raise "Reviewer needs a last name."
            if 'MiddleNames' in reviewer:
                Middlenames = reviewer['MiddleNames']
                if Middlenames is None:
                    Middlenames = ''
            else:
                Middlenames = ''
            if 'Institute' in reviewer:
                Institute = reviewer['Institute']
                if Institute is None:
                    Institute = ''
            else:
                Institute = ''
            if 'Email' in reviewer:
                Email = reviewer['Email']
                if Email is None:
                    raise "Reviewer needs an email."
            else:
                Email = ''
            if 'ScholarID' in reviewer:
                ScholarID = reviewer['ScholarID']
                if ScholarID is None:
                    ScholarID = ''
            else:
                ScholarID = ''
            if 'Nominator' in reviewer:
                Nominator = reviewer['Nominator']
                if Nominator is None:
                    Nominator = ''
            else:
                Nominator = ''

            add_reviewer = True
            if check_email:
                with conn:
                    cur = conn.cursor()
                    cur.execute("SELECT ID FROM Reviewers WHERE Email='" + Email + "'")
                    rows = cur.fetchall()
                    if rows:
                        add_reviewer = False
                        print('Reviewer with Email ' + Email + ' already exists.')
            if add_reviewer:
                add_string = '(\'' + Firstname.strip().replace("'", "''") + '\', \'' + Middlenames.strip().replace("'", "''") + '\', \'' + Lastname.strip().replace("'", "''") + '\', \'' + Institute.strip().replace("'", "''") + '\', \'' + Email.strip().replace("'", "''").lower() + '\', \'' + ScholarID.strip().replace("'", "''") + '\', \'' +  Nominator.strip().replace("'", "''") + '\', 1)'
                conn.execute("""INSERT INTO Reviewers (FirstName, MiddleNames, LastName, Institute, Email, ScholarID, Nominator, Active)
                                VALUES """ + add_string);
        conn.commit()
        conn.close()

    def email_id(self, email=None):
        return self._execute_sql("SELECT ID FROM Reviewers WHERE Email='" + email.strip().lower() + "'")

    def all_email_id(self, email=None):
        ids=self._execute_sql("SELECT ID FROM Reviewers WHERE Email='" + email.strip().lower() + "'")
        if not ids:
            ids = self._execute_sql("SELECT ID FROM Reviewers WHERE OtherEmails LIKE '%" + email.strip().lower() + "%'")
        return ids

    def ids(self):
        return self._execute_sql("SELECT ID FROM Reviewers")

    def lastname_id(self, lastname=None):
        return self._execute_sql("SELECT ID FROM Reviewers WHERE LastName=\"" + lastname.strip() + "\"")

    def firstname_id(self, firstname=None):
        return self._execute_sql("SELECT ID FROM Reviewers WHERE FirstName=\"" + firstname.strip() + "\"")

    def field_available(self, fieldname, table='Reviewers'):
        """Check if the given field is available in the data base."""
        field_available = False
        if fieldname:
            columns = self._execute_sql("""PRAGMA table_info(""" + table + """);""")
            for column in columns:
                if fieldname in column:
                    field_available = True
                    break
        return field_available


    def add_field(self, fieldname=None, fieldtype='NUMERIC', table='Reviewers'):
        """Check if a field is available already, and if not, add it to the data base."""
        if not self.field_available(fieldname, table):
            ans = self._execute_sql('ALTER TABLE ' + table + ' ADD ' + fieldname + ' ' + fieldtype + ';', commit=True)
        else:
            ans = None
        return ans

    def firstname_lastname_id(self, firstname=None, lastname=None):
        return self._execute_sql("SELECT ID FROM Reviewers WHERE LastName=\"" + lastname + "\" AND FirstName=\"" + firstname + "\"")

    def list_emails_from_ids(self, ids):
        """Return semi-colon separate list of emails associated with a given list of IDs."""
        email =''
        for id in ids:
            if len(email) > 0:
                email += ';'
            email += self._execute_sql("""SELECT Email FROM Reviewers WHERE ID=""" + str(id) + """;""")[0][0]
        return email

    def export_cmt(self, ids, filename='cmt_export.tsv'):
        """Export the given list of ids to a format suitable for importing into CMT."""
        outputfile= os.path.join(cmt_data_directory,filename)

        conn = sqlite3.connect(self.dbfile)
        rows = []
        for id in ids:
            cur = conn.execute("SELECT FirstName, MiddleNames, LastName, Email, Institute, ScholarID from Reviewers WHERE ID = " + str(id[0]))
            rows.append(cur.fetchall())
        output="First Name	Middle Initial	Last Name	Email	Organization\n"
        for row in rows:
            output+= row[0][0]+ '\t'+ row[0][1]+ '\t'+ row[0][2]+ '\t'+ row[0][3]+ '\t'+ row[0][4] + '\t' + 'http://scholar.google.com/citations?user='+row[0][5] + '\n'

        f = open(outputfile, 'w')
        f.write(output.encode('utf8'))
        f.close()

    def export_nips_papers(self, filename='cmt_export.tsv', sql='Active=1'):
        outputfile= os.path.join(cmt_data_directory,filename)

        conn = sqlite3.connect(self.dbfile)
        rows = []
        cur = conn.execute("SELECT FirstName, MiddleNames, LastName, Email, Institute, ScholarID, PapersSince2012 from Reviewers WHERE " + sql + ";")
        rows = cur.fetchall()
        output="First Name\tMiddle Initial\tLast Name\tEmail\tOrganization\tScholarID\tPapers Since 2012\n"
        for row in rows:
            if row[5]:
                output+= row[0]+ '\t'+ row[1]+ '\t'+ row[2]+ '\t'+ row[3]+ '\t'+ row[4] + '\t' + 'http://scholar.google.com/citations?user=' + row[5] + '\t' + unicode(row[6]) + '\n'
            else:
                output+= row[0]+ '\t'+ row[1]+ '\t'+ row[2]+ '\t'+ row[3]+ '\t'+ row[4] + '\t\t' + unicode(row[6]) + '\n'

        f = open(outputfile, 'w')
        f.write(output.encode('utf8'))
        f.close()

    def export_reviewers(self, filename='cmt_export.tsv', sql='Active=1'):
        outputfile= os.path.join(cmt_data_directory,filename)

        conn = sqlite3.connect(self.dbfile)
        cur = conn.execute("SELECT FirstName, MiddleNames, LastName, Email, Institute, ScholarID from Reviewers WHERE " + sql + ";")
        rows = cur.fetchall()
        output="First Name	Middle Initial	Last Name	Email	Organization	URL\n"
        for row in rows:
            if row[5]:
                output+= row[0]+ '\t'+ row[1]+ '\t'+ row[2]+ '\t'+ row[3]+ '\t'+ row[4] + '\t' + 'http://scholar.google.com/citations?user=' + row[5] + '\n'
            else:
                output+= row[0]+ '\t'+ row[1]+ '\t'+ row[2]+ '\t'+ row[3]+ '\t'+ row[4] + '\t\n'

        f = open(outputfile, 'w')
        f.write(output.encode('utf8'))
        f.close()

    def export_tpm(self, filename='tpm_export.csv', sql='IsReviewer=1'):
        outputfile= os.path.join(cmt_data_directory,filename)

        conn = sqlite3.connect(self.dbfile)
        cur = conn.execute("SELECT Email, LastName, FirstName from Reviewers WHERE " + sql + ";")
        rows = cur.fetchall()
        output=""#First Name	Middle Initial	Last Name	Email	Organization	URL\n"
        for row in rows:
            output+= row[0]+ ','+ row[1]+ ','+ row[2]+ '\n'

        f = open(outputfile, 'w')
        f.write(output.encode('utf8'))
        f.close()


def normalise(word):
    """Normalise word into lower case and lemmatizes it."""
    import nltk
    lemmatizer = nltk.WordNetLemmatizer()
    stemmer = nltk.stem.porter.PorterStemmer()
    #from nltk.corpus import stopwords
    #stopwords = stopwords.words('english')

    delim = '\t'
    word = word.lower()
    word = lemmatizer.lemmatize(word)
    word = stemmer.stem_word(word)
    return word

def acceptable_word(word):
    """Checks conditions for acceptable word: length, stopword."""
    from nltk.corpus import stopwords
    stopwords = stopwords.words('english')
    accepted = bool(2 <= len(word) <= 40
        and word.lower() not in stopwords)
    return accepted

def extract_stem_words(str):
    import nltk
    words = list()
    for sentence in nltk.tokenize.sent_tokenize(str):
        sentence = sentence.replace('---', ' ')
        sentence = sentence.replace('/', ' ')
        sentence = sentence.replace('-', ' ')
        sentence = sentence.replace("``", ' ')
        sentence = sentence.replace("`", ' ')
        sentence = sentence.replace("''", ' ')
        sentence = sentence.replace("'", ' ')
        sentence = sentence.replace("e.g.", ' ')
#        sentence = sentence.replace('''', ' ')
        for word in nltk.tokenize.word_tokenize(sentence):
            wrd = normalise(word)
            if acceptable_word(wrd):
                words.append(wrd)
    return words
