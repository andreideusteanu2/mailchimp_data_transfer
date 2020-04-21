# Mailchimp Data Transfer
This is a small ETL app that extract newlsetter campaigns data from Mailchimp and transfers it for storage in Google BigQuery.

There are multiple data components regarding a campaign. The components can be identfied in the etl.py file.

The Mailchimp API at 'https://us1.api.mailchimp.com/3.0/' is slow to respond therefore this app uses both batching and parallel processing to make web requests to it.
The parrallel processing is handled by the multiprocess Python library.
Data transformation consists of creating Python dictionaries and combining them in a logical manner based on the response of the API.
In the end you should get 2 BigQuery tables 'email_campaigns' and 'email_campaigns_activity' which are joinable as follows:
- email_campaigns.Id=email_campaigns_activity.CampaignId


The steps for running this are:
1. In the app/credentials directory add 2 files:
  a. The Google Cloud Service Account Key json file
  b. The Mailchimp API Key as a json file in the format {"key":"[replace key here]"}
2. In the app/etl.py replace the quoted string with the file names for the files created at point 1.
3. Build the container:
  a. Change to the mailchimp_data_transfer directory on the machine
  b. Run the command docker build -t mailchimp_data_transfer:latest .
4. Run the container:
  a. docker run mailchimp_data_transfer:latest
