import os
from pathlib import Path
from typing import Dict, Any, Optional, List

from dotenv import load_dotenv

class Config:
    """Configuration class for ETL pipeline."""
    
    def __init__(self, env_file: Optional[Path] = None):
        """
        Initialize configuration.
        
        Args:
            env_file: Path to .env file
        """
        # Load environment variables from .env file if it exists
        if env_file and env_file.exists():
            load_dotenv(env_file)
            
        # Database URIs - prioritize environment variables
        self.prod_db_uri = os.environ.get('PROD_DB_URI')
        self.dev_db_uri = os.environ.get('DEV_DB_URI')
        
        # ETL settings
        self.batch_size = int(os.environ.get('BATCH_SIZE', '100'))
        
        # Print configuration source
        print("Configuration loaded from environment variables")
        if not self.prod_db_uri or not self.dev_db_uri:
            print("Warning: Database URIs not found in environment variables")
        
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'Config':
        """
        Create configuration from dictionary.
        
        Args:
            config_dict: Dictionary with configuration values
            
        Returns:
            Config instance
        """
        config = cls()
        
        for key, value in config_dict.items():
            if hasattr(config, key):
                setattr(config, key, value)
                
        return config

    def validate(self) -> List[str]:
        """
        Validate configuration.
        
        Returns:
            List of validation errors
        """
        errors = []
        
        # Required settings
        if not self.prod_db_uri:
            errors.append("Production database URI is required")
        
        if not self.dev_db_uri:
            errors.append("Development database URI is required")
            
        # Validation checks
        if self.prod_db_uri and not self.prod_db_uri.startswith('postgresql://'):
            errors.append("Production database URI must be PostgreSQL")
            
        if self.dev_db_uri and not self.dev_db_uri.startswith('postgresql://'):
            errors.append("Development database URI must be PostgreSQL")
            
        if self.batch_size <= 0:
            errors.append("Batch size must be positive")
            
        return errors