import streamlit as st
import pandas as pd
from trust_pilot_scraper import TrustPilotScraper

# Streamlit app

st.title("TrustPilot Review Scraper")

st.image("assets/tp_logo_new.png",width=300)

# Input URL
url = st.text_input("Enter the full TrustPilot URL to scrape reviews from:",
                    help="Enter the URL of the TrustPilot page to scrape reviews from e.g.https://uk.trustpilot.com/review/www.tesco.com")

# Input number of pages
num_pages = st.number_input("Enter the number of pages to scrape:", value=1, min_value=1, step=1,
                            help="Note - this is limited to a maximum of twenty five pages to prevent misuse")

if num_pages > 25:
    num_pages = 25

if st.button("Scrape Reviews"):
    if url:
        # Initialize the TrustPilot scraper
        scraper = TrustPilotScraper(url, num_pages)

        # Scrape reviews and transform into a dataframe
        scraped_reviews = scraper.run()

        if isinstance(scraped_reviews, pd.DataFrame):
            # Display scraped reviews
            st.write("Scraped Reviews:")
            st.dataframe(scraped_reviews)
            st.download_button("Download CSV", data=scraped_reviews.to_csv(), file_name="scraped_reviews.csv")

        else:
            st.write("No reviews scraped.")