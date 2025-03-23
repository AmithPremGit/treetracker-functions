import argparse
import sys
from pathlib import Path

from config import Config
from pipeline import ETLPipeline

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='ETL Pipeline to copy data from production to development environment'
    )
    
    parser.add_argument(
        '--prod-db-uri', 
        help='Production database URI (overrides environment variable)'
    )
    
    parser.add_argument(
        '--dev-db-uri', 
        help='Development database URI (overrides environment variable)'
    )
    
    parser.add_argument(
        '--batch-size', 
        type=int, 
        help='Batch size for processing records (default: 100)'
    )
    
    parser.add_argument(
        '--tables', 
        nargs='*',
        help='Optional list of specific tables to copy. If not provided, all tables will be copied.'
    )

    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Perform a dry run without making changes to the dev database'
    )
    
    parser.add_argument(
        '--env-file',
        type=Path,
        help='Path to .env file with configuration'
    )
    
    return parser.parse_args()

def main():
    """Main entry point for the ETL pipeline."""
    args = parse_arguments()
    
    # Load configuration from env file or environment variables
    config = Config(args.env_file if args.env_file else None)
    
    # Command line arguments only override if explicitly provided
    config_dict = {}
    if args.prod_db_uri:
        config_dict['prod_db_uri'] = args.prod_db_uri
    if args.dev_db_uri:
        config_dict['dev_db_uri'] = args.dev_db_uri
    if args.batch_size:
        config_dict['batch_size'] = args.batch_size
    
    # Update config with command line arguments
    if config_dict:
        for key, value in config_dict.items():
            setattr(config, key, value)
    
    # Validate configuration
    errors = config.validate()
    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        return 1
    
    try:
        # Initialize and run the ETL pipeline
        pipeline = ETLPipeline(
            prod_db_uri=config.prod_db_uri,
            dev_db_uri=config.dev_db_uri,
            batch_size=config.batch_size,
            tables=args.tables,
            dry_run=args.dry_run
        )
        
        pipeline.run()
        
        print("ETL Pipeline completed successfully")
        return 0
        
    except Exception as e:
        print(f"ETL Pipeline failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())