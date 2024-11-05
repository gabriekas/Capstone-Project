# Customer Behavioural Analysis For "The Look" E-Commerce Business In 2023

## Project Description
In the rapidly evolving eCommerce landscape, customer behavioural analysis is crucial for businesses to remain competitive. Thus, current project aimed to answer how a fictitious E-Commerce clothing site "The Look" could maintain its industry competetiviness. To answer this question, three key methodologies were employed:   
- **_Funnel Analysis_**  
- **_RFM (recency, frequency and monetary) Customer Segmentation_**  
- **_Cohort Analysis_**  

SQL queries were developed to aggregate data for the above analyses implementation. All aggregated data was exported as CSV files on September 12th, 2024, to maintain continuity as historical data undergoes daily updates. Thereafter, the data was imported to Tableau  for visualisation. The data was displayed in an interactive dashboard with four main sections: an overview, funnel analysis, RFM segmentation, and cohort analysis. The preview of the dashboard is available on [Tableau Public](https://public.tableau.com/views/TheLookEcommerceCustomerBehaviourIn2023/OVERVIEW?:language=en-GB&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link).   


## Dataset Information
- **Source**: publicly available dataset on [BigQuery](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce?project=gabriele-transfer)  
- **Description**: The dataset used for this analysis is a synthetic dataset developed by the Looker team for “The Look”. The dataset is designed to mimic the real world eCommerce environment and offers insights into business dynamics and customer behaviour. It includes diverse information about customers, products, orders, logistics, and digital marketing campaigns across 7 distinct tables. The historical data undergoes daily updates. The data span from late 2018 to 2024, covering information from orders to logistics, with tables containing up to  2,429,608 rows.  

## Disclaimer
_The project was performed for learning purposes. Insights should not be taken as a professional advice._  

_A human error occured where **Deutschland** was mistakenly identified as **Dutch**. Due to the daily updates of historical data, this error was not solved at the time of the analysis. For the purposes of this analysis, **Deutschland** is assumed to refer to the **Netherlands**._  

## Prerequisites
To run the project, the following is required:  
- SQL  
- Tableau Public  
- Google Docs  

## Key Takeaways
The analysis identified a mix of strong financial performance and high initial customer engagement, alongside challenges in customer retention, product quality and data collection. Several key recommendations were made to address the challenges and help "The Look" to maintain its competetiviness.  

## Recommendations for "The Look" Data and Marketing Teams
- Data audit to improve accuracy in order tracking and metrics.  
- Gather the data on the checkout processes and identify whether cart abandonment reminders are in place.  
- Personalised marketing campaigns tailored to specific customer segments.  
- Country specific marketing strategies to target underperforming regions.  
- Customer surveys to gather feedback from high order return and cancellation segments.  
- Further exploration of checkout processes, product quality, and Customer Lifetime Value (CLV) as well as the implementation of A/B testing to drive continuous improvement.  

# Access Analysis Report and Interactive Dashboard

- [Report of The Look E-Commerce Customer Behaviour Analysis In 2023](https://docs.google.com/document/d/15aqCreWAeGGJLCL5gSx_dJ5vmc1XNrrN-hocH_Z540k/edit?usp=sharing)  

- [Interactive Tableau Dashboard](https://public.tableau.com/views/TheLookEcommerceCustomerBehaviourIn2023/OVERVIEW?:language=en-GB&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link)  
