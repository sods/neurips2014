# Utility functions for processing NIPS reviews.
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt
from datetime import timedelta
import numpy as np
import os


# Date of different review events.
events = {}
# Time stamps from CMT are on separate time? If so add here
offset = dt.timedelta(hours=0)
events['reviews'] = dt.datetime(2014, 7, 21, 23, 59) + offset
events['rebuttal_start'] = dt.datetime(2014, 8, 3, 23, 59) + offset
events['rebuttal_ends'] = dt.datetime(2014, 8, 11, 23, 59) + offset
events['start_teleconference'] = dt.datetime(2014, 8, 19, 23, 59) +offset
events['decisions_despatched'] = dt.datetime(2014, 9, 5, 23, 59) + offset

# Date range across which we have review information.
review_date_range = pd.date_range('2014/07/01', periods=72, freq='D')
review_store = ''
dir_store = os.path.join(os.environ['HOME'], review_store)

def load_review_history():
    """Load in the history of the NIPS reviews."""

    # return load of pickled reviews.
    return pd.io.pickle.read_pickle(os.path.join(dir_store, 'all_reviews.pickle'))

def reviews_before(reviews, date):
    "Give a review snapshot of reviews before a certain date."
    indices = (((reviews.LastUpdated<=date) & (reviews.LastSeen>date))
               | ((reviews.LastUpdated<=date) & (reviews.LastSeen.isnull())))
    return reviews[indices].sort(columns='LastUpdated').drop_duplicates(subset=['Email', 'ID'], take_last=True)

def reviews_status(reviews, datetime, column=None):
    """Give a snapshot of the reviews at any given time. Use multi-index across ID
    and Email"""

    if column:
        return reviews_before(reviews, datetime).set_index(['ID', 'Email'])[column].sort_index()
    else:
        return reviews_before(reviews, datetime).set_index(['ID', 'Email']).sort_index()

def plot_deadlines(ax):
    "Plot the deadlines for the different reviewing stages"
    for event in events.keys():
        plt.axvline(events[event])
    ax.set_xticks(events.values())
    ax.set_xticklabels(events.keys(), rotation=90 )


def evolving_statistic(reviews, column, window=4):
    "Plot a particular review statistic mean as it evolves over time."
    first_entered = reviews.sort(columns='LastUpdated', ascending=False).drop_duplicates(subset=['ID', 'Email'],take_last=True).sort(columns='LastUpdated')

    df = pd.DataFrame(index=review_date_range, columns=[column + ' mean',
                                                              column + ' std',
                                                              'Number of Reviews'])
    for date in review_date_range:
        indices = (first_entered.LastUpdated<date+timedelta(window/2.)) & (first_entered.LastUpdated>date-timedelta(window/2.))
        df['Number of Reviews'][date] = indices.sum()
        if indices.sum()>2:
            df[column + ' mean'][date] = first_entered[column][indices].mean()
            df[column + ' std'][date] = 2*np.sqrt(first_entered[column][indices].var()/indices.sum())
        else:
            df[column + ' mean'][date] = np.NaN
    ax = df[column + ' mean'].plot(yerr=df[column + ' std'])
    plot_deadlines(ax)

    indices = (reviews.LastUpdated<events['reviews'])

def late_early_statistic(reviews, column, ylim):
    "Compute a statistic for late reviews and a statistic for early reviews"
    first_entered = reviews.sort(columns='LastUpdated', ascending=False).drop_duplicates(subset=['ID', 'Email'],take_last=True).sort(columns='LastUpdated')
    cat1 = first_entered[column][first_entered.LastUpdated<events['reviews']]
    print "On time reviewers", column + ":", cat1.mean(), '+/-', 2*np.sqrt(cat1.var()/cat1.count())
    cat2 = first_entered[column][(first_entered.LastUpdated>events['reviews'])& (first_entered.LastUpdated < events['rebuttal_start'])]
    print "Chased reviewers", column + ":", cat2.mean(), '+/-', 2*np.sqrt(cat2.var()/cat2.count())
    fix, ax = plt.subplots()
    ax.bar([0.6, 1.6],
           [cat1.mean(), cat2.mean()],
           color ='y', width = 0.8,
           yerr=[2*np.sqrt(cat1.var()/cat1.count()), 2*np.sqrt(cat2.var()/cat2.count())])
    ax.set_ylim(ylim[0], ylim[1])
    ax.set_title('Mean ' + column + ' for Reviews')
    ax.set_xticks([1, 2])
    ax.set_xticklabels(['On time reviews', 'Late Reviews'])
    from scipy.stats import ttest_ind
    vals = ttest_ind(cat1, cat2)
    print "t-statistic is", vals[0], "and p-value is", vals[1]

# def top_papers(reviews):
#     """Compute the top review levels."""
#     for date in review_date_range:
        
