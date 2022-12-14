from pymongo import MongoClient

import dash_core_components as dcc
import dash_html_components as html
import dash
import pandas as pd
import numpy as np
import re
from tqdm import tqdm
import time
from dask.distributed import Client, progress
import joblib
from dash.dependencies import Input, Output
import plotly.express as px

app = dash.Dash('Dashboard')

pd.set_option('display.max_rows', None)

def HotelReviews():
    print('<-- Fetching Hotel Reviews -->' + "\n")
    
    # hotel reviews
    hotel_set = pd.read_csv("Hotel_Reviews.csv")

    # # only get 1 percent of data.
    hotel_set = hotel_set.sample(
        frac=0.01, replace=False, random_state=42)

    # Remove invaluable reviews
    remove_list = ['No Negative', 'No Positive',
                   ' Nothing', ' Nothing ', ' nothing', ' None', ' N A']

    # replace all 'No Negative' and 'No Positive' from review set with an empty string
    for x in remove_list:
        hotel_set = hotel_set.replace([x], [''])

    # replace all empty strings with NaN values
    replace_df = hotel_set.replace(r'^\s*$', np.nan, regex=True)

    # drop all replaced values
    hotel_set = replace_df.dropna()
    
    # Get new review set
    all_reviews = hotel_set[["Hotel_Address", "Hotel_Name", "lat", "lng","Average_Score", 
                           "Reviewer_Score", "Review_Date", "Reviewer_Nationality", "Positive_Review", "Negative_Review"]]
            
    return all_reviews


def CleanReviews(all_review_set):
    print("\n" + '<-- Cleaning Reviews -->' + "\n")
    
    review_data = all_review_set

    # Remove custom words
    unwanted_words = ['room', 'staff', 'locat',
                      'hotel', 'breakfast', 'bed', 'shower', 'ion', 'building', ]
    review_data['Positive_Review'] = review_data['Positive_Review'].str.replace(
        '|'.join(map(re.escape, unwanted_words)), '')
    review_data['Negative_Review'] = review_data['Negative_Review'].str.replace(
        '|'.join(map(re.escape, unwanted_words)), '')
    
    review_data.replace("", float("NaN"), inplace=True)
    review_data.dropna(subset=["Positive_Review"], inplace=True)
    review_data.dropna(subset=["Negative_Review"], inplace=True)


    return review_data


def ReviewsToDB(review_data):
  
    server = MongoClient('mongodb://root:root@127.0.0.1:27017')

    db = server.db
    reviews = db.reviews
    
    for index, row in review_data.iterrows():
        with joblib.parallel_backend('dask'):
            reviews.insert_one(({'Hotel_Address':row['Hotel_Address'],
                    'Hotel_Name':row['Hotel_Name'],
                    'lat':row['lat'],
                    'lng':row['lng'],
                    'Average_Score': row['Average_Score'],
                    'Reviewer_Score':row['Reviewer_Score'],
                    'Review_Date':row['Review_Date'],
                    'Reviewer_Nationality':row['Reviewer_Nationality'],
                    'Positive_Review':row['Positive_Review'],
                    'Negative_Review':row['Negative_Review']
                }))
    
    print('<-- Uploaded reviews to MONGODB Database -->' + "\n")


def Dashboard():
    app = dash.Dash('Dashboard')
    df = pd.DataFrame({
    "Fruit": ["Apples", "Oranges", "Bananas", "Apples", "Oranges", "Bananas"],
    "Amount": [4, 1, 2, 2, 4, 5],
    "City": ["SF", "SF", "SF", "Montreal", "Montreal", "Montreal"]
})

    fig = px.bar(df, x="Fruit", y="Amount", color="City", barmode="group")

    app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for your data.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])


if __name__ == "__main__":
    client = Client()
    all_review_set = HotelReviews()
    # print(all_review_set)
    review_data = CleanReviews(all_review_set)    
    ReviewsToDB(review_data)
    app.run_server(debug=True)