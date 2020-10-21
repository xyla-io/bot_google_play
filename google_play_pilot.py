import pandas as pd

from raspador import OrdnancePilot, UserInteractor, BrowserInteractor
from typing import Dict, Optional
from pathlib import Path

class GooglePlayPilot(OrdnancePilot[pd.DataFrame]):
  config: Dict[str, any]
  sign_in_wait = 3.0
  _download_path: Optional[Path]

  def __init__(self, config: Dict[str, any], user: UserInteractor, browser: BrowserInteractor):
    self.config = config
    self._download_path = None
    super().__init__(user=user, browser=browser)
  
  @property
  def email(self) -> str:
    return self.config['email']

  @property
  def password(self) -> str:
    return self.config['password']
  
  @property
  def company_name(self) -> str:
    return self.config['company_name']
  
  @property
  def app_id(self) -> str:
    return self.config['app_id']
  
  @property
  def days_back(self) -> int:
    return self.config['days']
  
  @property
  def slackbot_api_token(self) -> str:
    return self.config['slackbot_api_token']
  
  @property
  def download_path(self) -> Path:
    if self._download_path:
      return self._download_path

    user_directory_path = Path(f'output/google_play/{self.app_id}')
    if not user_directory_path.exists():
      user_directory_path.mkdir()
  
    download_path = user_directory_path / self.user.date_file_name()
    download_path.mkdir()
    self._download_path = download_path
    return download_path