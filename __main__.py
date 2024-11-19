import logging
import argparse
import asyncio
import time
from pathlib import Path 


# Import custom modules
from config import CA_NAMES, OUTPUT_PATH, OUTPUT_INVALID_PATH
from data.scraper import SINACPayloadSraper, SINACRedScraper

# This file was used to check the scraping process and the data extraction process

def parse_arguments():
    """Set up command-line argument parsing."""
    parser = argparse.ArgumentParser(description="Scrape water quality data.")
    
    parser.add_argument(
        "--com_ids", 
        type=int, 
        nargs='+',  # This allows multiple integers to be passed directly
        default=list(range(1, len(CA_NAMES) + 1)),
        help="List of community IDs to process (default: all)."
    )
    parser.add_argument(
        "--skip_payload",
        action='store_true',
        default=False,
        help="Decide whether or not to skip the payload generation. Must only be used if the last execution contained the necessary payloads"
    )
    parser.add_argument(
        "--output_data", 
        type=str, 
        default=OUTPUT_PATH, 
        help="Path to save the CSV output file."
    )
    parser.add_argument(
        "--output_invalid", 
        type=str, 
        default=OUTPUT_INVALID_PATH, 
        help="Path to save the CSV file containing invalid municipalities."
    )
    parser.add_argument(
        "--log", 
        type=str, 
        choices=["debug", "info", "warning", "error"],
        default="info", 
        help="Set the logging level."
    )
    
    return parser.parse_args()

def save_by_community(data_df, invalid_mun_df, args):
    """Save the data and invalid municipalities by community."""
    for com_id in args.com_ids:
        com_name = CA_NAMES[com_id-1]
        com_data_df = data_df[data_df['Comunidad Autónoma'] == com_name]
        com_invalid_mun_df = invalid_mun_df[invalid_mun_df['Comunidad Autónoma'] == com_name]

        data_filepath = Path(args.output_data.replace("/", f"/{com_name}/")) 
        data_filepath.parent.mkdir(parents=True, exist_ok=True) 
        invalid_filepath = Path(args.output_invalid.replace("/", f"/{com_name}/")) 
        invalid_filepath.parent.mkdir(parents=True, exist_ok=True) 

        com_data_df.to_csv(data_filepath, index=False)
        com_invalid_mun_df.to_csv(invalid_filepath, index=False)

async def main(args):
    print("Community names to process:", [CA_NAMES[i-1] for i in args.com_ids])
    if args.skip_payload:
        payload_df = None
        print("Payload generation skipped. Using the last generated payloads.")
    else:
        payload_time = time.time()
        payload_scraper = SINACPayloadSraper(args.com_ids)
        payload_df = await payload_scraper.scrape()
        print("Time taken to extract", len(payload_df), "payloads:", time.time() - payload_time)
        print("\n\n---------------EXTRACTED ALL THE REQUIRED PAYLOADS---------------\n\n")
    data_time = time.time()
    data_scraper = SINACRedScraper(payload_df)
    data_df, invalid_mun_df = await data_scraper.scrape()
    print("Time taken to extract", len(data_df), "data points:", time.time() - data_time)
    print("\n\n---------------EXTRACTED ALL THE REQUIRED DATA---------------\n\n")
    if len(args.com_ids) != len(CA_NAMES):
        save_by_community(data_df, invalid_mun_df, args)
    else:
        data_df.to_csv(args.output_data, index=False)
        invalid_mun_df.to_csv(args.output_invalid, index=False)
    
    print("Data saved to output directory.")
    return data_df, invalid_mun_df


if __name__ == "__main__":
    args = parse_arguments()
    asyncio.run(main(args))