import re
import datetime
import pandas as pd
import importlib
import calendar
import os
import slack

from google_play.google_play_pilot import GooglePlayPilot
from google_play.google_play_processor import GooglePlayProcessor
from raspador import Maneuver, OrdnanceManeuver, NavigationManeuver, SequenceManeuver, UploadReportRaspador, ClickXPathSequenceManeuver, InteractManeuver, OrdnanceParser, XPath, RaspadorNoOrdnanceError, ClickXPathManeuver, SeekParser, SoupElementParser, FindElementManeuver, ClickSoupElementManeuver, Element, ClickElementManeuver
from typing import Generator, Optional, Dict, List, Tuple
from time import sleep
from bs4 import BeautifulSoup
from pathlib import Path
from zipfile import ZipFile

import google_play.google_play_parser
importlib.reload(google_play.google_play_parser)
from google_play.google_play_parser import DateNotAvailableParser, AcquisitionReportParser

class SignInManeuver(Maneuver[GooglePlayPilot]):
  def attempt(self, pilot: GooglePlayPilot):
    user_input = pilot.browser.get_clickable(xpath="//input[@id='identifierId']")
    user_input.click()
    user_input.send_keys(pilot.email)
    next_button = pilot.browser.get_clickable(xpath="//div[@id='identifierNext']")
    next_button.click()
    sleep(pilot.sign_in_wait)
    password_input = pilot.browser.get_clickable(xpath="//div[@id='password']/descendant::input[@type='password']")
    password_input.click()
    password_input.send_keys(pilot.password)
    password_next_button = pilot.browser.get_clickable(xpath="//div[@id='passwordNext']")
    password_next_button.click()
    sleep(pilot.sign_in_wait)

class CheckDateNotAvailableManeuver(OrdnanceManeuver[GooglePlayPilot, bool]):
  def attempt(self, pilot: GooglePlayPilot):
    date_panel_xpath = "//button[@aria-label='Cohort dates selector.']/following-sibling::div"
    date_panel = Element(element=pilot.browser.get_visible(date_panel_xpath))
    try:
      parser = DateNotAvailableParser(date_panel.source)
      date_not_available_xpath = f'{date_panel_xpath}/{parser.parse().deploy()}'
      self.ordnance = pilot.browser.get_visible(date_not_available_xpath, timeout=2.0) is not None
    except RaspadorNoOrdnanceError:
      self.ordnance = False

class FindLastDateAvailableManeuver(OrdnanceManeuver[GooglePlayPilot, datetime.date]):
  def attempt(self, pilot: GooglePlayPilot):
    date_panel_xpath = "//button[@aria-label='Cohort dates selector.']/following-sibling::div"
    date_panel = Element(element=pilot.browser.get_visible(date_panel_xpath))

    def seek_last_day(parser):
      return parser.soup.find_all('a', text=re.compile(r'^[0-9]{1,2}$'))[-1]

    last_day_element = (yield FindElementManeuver(
      parser=SoupElementParser(
        instruction='Find last day',
        seeker=seek_last_day,
        source=date_panel.source
      )
    )).deploy()

    def seek_month_and_year(parser):
      return parser.soup.find_all('span', text=re.compile(r'^\w+ [0-9]{4}$'))[-1]

    month_and_year_element = (yield FindElementManeuver(
      parser=SoupElementParser(
        instruction='Find month and year',
        seeker=seek_month_and_year,
        source=date_panel.source
      )
    )).deploy()
    
    last_date_text = f'{last_day_element.text} {month_and_year_element.text}'.strip()
    self.ordnance = datetime.datetime.strptime(last_date_text, '%d %B %Y').date()

class LastDateAvailableManeuver(OrdnanceManeuver[GooglePlayPilot, datetime.date]):
  def attempt(self, pilot: GooglePlayPilot):
    # set the date breakdown to 'Day'
    date_selector_xpath = "//button[@aria-label='Cohort dates selector.']"
    date_panel_xpath = f'{date_selector_xpath}/following-sibling::div'
    yield ClickXPathSequenceManeuver(xpaths=[
      date_selector_xpath,
      f"{date_panel_xpath}/descendant::button"
    ])

    while True:
      date_not_available = yield CheckDateNotAvailableManeuver()
      if date_not_available.deploy():
        break

      yield ClickXPathManeuver(xpath=f"{date_panel_xpath}/descendant::button[@aria-label='Next page']")

    self.ordnance = (yield FindLastDateAvailableManeuver()).deploy()

class ScrapeMonthAndYearManeuver(OrdnanceManeuver[GooglePlayPilot, datetime.date]):
  def attempt(self, pilot: GooglePlayPilot):
    date_panel_xpath = "//button[@aria-label='Cohort dates selector.']/following-sibling::div"
    date_panel = Element(element=pilot.browser.get_visible(date_panel_xpath))

    def seek_month_and_year(parser):
      return parser.soup.find_all('span', text=re.compile(r'^\w+ [0-9]{4}$'))[-1]

    month_and_year_element = (yield FindElementManeuver(
      parser=SoupElementParser(
        instruction='Find month and year',
        seeker=seek_month_and_year,
        source=date_panel.source
      )
    )).deploy()
    
    self.ordnance = datetime.datetime.strptime(month_and_year_element.text.strip(), '%B %Y').date()

class SelectMonthAndYearManeuver(Maneuver[GooglePlayPilot]):
  month_and_year: datetime.date

  def __init__(self, month_and_year: datetime.date):
    self.month_and_year = datetime.date(month_and_year.year, month_and_year.month, 1)
    super().__init__()

  def attempt(self, pilot: GooglePlayPilot):
    date_panel_xpath = "//button[@aria-label='Cohort dates selector.']/following-sibling::div"

    while True:
      month_and_year = (yield ScrapeMonthAndYearManeuver()).deploy()
      if month_and_year < self.month_and_year:
        yield ClickXPathManeuver(f"{date_panel_xpath}/descendant::button[@aria-label='Next page']")
      elif month_and_year > self.month_and_year:
        yield ClickXPathManeuver(f"{date_panel_xpath}/descendant::button[@aria-label='Previous page']")
      else:
        break

class SelectDateManeuver(Maneuver[GooglePlayPilot]):
  date: datetime.date

  def __init__(self, date: datetime.date):
    self.date = date
    super().__init__()

  def attempt(self, pilot: GooglePlayPilot):
    date_panel_xpath = "//button[@aria-label='Cohort dates selector.']/following-sibling::div"
    date_panel = Element(element=pilot.browser.get_visible(date_panel_xpath))

    yield SelectMonthAndYearManeuver(month_and_year=self.date)

    def seek_day(parser):
      return parser.soup.find_all('a', text=f'{self.date.day}')[-1].span
      
    clicked = yield ClickSoupElementManeuver(
      parser=SoupElementParser(
        instruction=f'Day {self.date.day}',
        seeker=seek_day,
        source=date_panel.source
      ),
      xpath_prefix=f'{date_panel_xpath}/'
    )

class ScrapeAcquisitionReportManeuver(OrdnanceManeuver[GooglePlayPilot, pd.DataFrame]):
  def attempt(self, pilot: GooglePlayPilot):
    parser = AcquisitionReportParser.from_browser(browser=pilot.browser)
    self.ordnance = parser.parse().deploy()

class ScrapeDateManeuver(OrdnanceManeuver[GooglePlayPilot, pd.DataFrame]):
  date: datetime.date

  def __init__(self, date: datetime.date):
    self.date = date
    super().__init__()

  def attempt(self, pilot: GooglePlayPilot):
    date_selector_xpath = "//button[@aria-label='Cohort dates selector.']"
    pilot.browser.get_clickable(xpath=date_selector_xpath).click()

    select_date = yield SelectDateManeuver(date=self.date) 
    self.require(select_date)
    sleep(5.0)
    self.ordnance = (yield ScrapeAcquisitionReportManeuver()).deploy()
    self.ordnance['date'] = self.date

class ScrapeDateRangeManeuver(OrdnanceManeuver[GooglePlayPilot, pd.DataFrame]):
  date_range: 'Range[datetime.date]'

  def __init__(self, date_range: 'Range[datetime.date]'):
    self.date_range = date_range
    super().__init__()

  def attempt(self, pilot: GooglePlayPilot):
    self.ordnance = pd.DataFrame()
    
    sequence = yield SequenceManeuver([
      ScrapeDateManeuver(date=d)
      for d in self.date_range
    ])
    for date in sequence.sequence:
      self.ordnance = self.ordnance.append(date.deploy())

class DownloadBulkExportManeuver(OrdnanceManeuver[GooglePlayPilot, List[Path]]):
  download_dates: Optional[List[datetime.datetime]]

  def __init__(self, download_dates: Optional[List[datetime.datetime]]=None):
    self.download_dates = download_dates
    super().__init__()

  def attempt(self, pilot: GooglePlayPilot):
    sleep(2)
    yield ClickElementManeuver(
      instruction='click the bulk export download button',
      seeker=lambda p: p.soup.find('a', {'href': f'#BulkExportPlace:bep={pilot.app_id}&bet=USER_ACQUISITION'})
    )
    sleep(5)

    for date in self.download_dates:
      yield ClickElementManeuver(
        instruction='click the year accordion',
        seeker=lambda p: p.soup.find('div', {'class': 'gwt-Label'}, text=re.compile(f'{date.year}'))
      )

      sleep(2)
      yield ClickElementManeuver(
        instruction='click the bulk export download button',
        seeker=lambda p: p.soup.find('button', {'aria-label': f'Download: Retained installers report, {calendar.month_name[date.month]} {date.year}'})
      )

      yield ClickElementManeuver(
        instruction='click the year accordion',
        seeker=lambda p: p.soup.find('div', {'class': 'gwt-Label'}, text=re.compile(f'{date.year}'))
      )
      sleep(2)

    current_dir = Path('.')
    downloaded_file_paths = [p for p in current_dir.glob(f'retained_installers_{pilot.app_id}*.zip')]

    renamed_downloaded_file_paths = []
    for file in downloaded_file_paths:
      target = f'{pilot.download_path.absolute()}/{file.name}'
      file.rename(target)
      renamed_downloaded_file_paths.append(Path(target))
    
    self.ordnance = renamed_downloaded_file_paths

class OpenClassicPlayConsoleManeuver(Maneuver[GooglePlayPilot]):
  def attempt(self, pilot: GooglePlayPilot):
    yield ClickElementManeuver(
      instruction='click on the "Use classic Play Console" button',
      seeker=lambda p: p.soup.find('span', {'class': 'label'}, text=re.compile('Use classic Play Console')).parent
    )

    sleep(5)
    pilot.browser.driver.close()
    assert len(pilot.browser.driver.window_handles) == 1
    pilot.browser.driver.switch_to.window(pilot.browser.driver.window_handles[0])

class ProcessGooglePlayDataManeuver(OrdnanceManeuver[GooglePlayPilot, GooglePlayProcessor]):
  def attempt(self, pilot: GooglePlayPilot):
    processor = GooglePlayProcessor(source_directory_path=pilot.download_path)
    processor.process()
    processor.save()
    self.ordnance = processor

class SendDataToSlackManeuver(Maneuver[GooglePlayPilot]):
  data_path: Path
  min_date: datetime.date
  max_date: datetime.date

  def __init__(self, data_path: Path, min_date: datetime.date, max_date: datetime.date):
    self.data_path = data_path
    self.min_date = min_date
    self.max_date = max_date
    super().__init__()

  def attempt(self, pilot: GooglePlayPilot):
    zipfile_path = f'{str(self.data_path.parent)}/{pilot.app_id}_{self.data_path.parent.name}.zip'
    with ZipFile(zipfile_path, 'w') as z:
      for csv_file_path in self.data_path.glob("*.csv"):
        z.write(csv_file_path)
      z.close()

    text = f'''
*Client:* {pilot.company_name}
*App ID:* `{pilot.app_id}`
*Dates:* `{self.min_date.strftime("%Y-%m-%d")}`—`{self.max_date.strftime("%Y-%m-%d")}`
'''

    client = slack.WebClient(token=pilot.slackbot_api_token)
    client.files_upload(
      channels='#xyla-devs',
      initial_comment=text,
      file=zipfile_path,
      filename=f'{pilot.app_id}_{self.min_date.strftime("%Y-%m-%d")}-{self.max_date.strftime("%Y-%m-%d")}.zip',
      filetype='zip'
    )

class GooglePlayManeuver(OrdnanceManeuver[GooglePlayPilot, pd.DataFrame]):
  def attempt(self, pilot: GooglePlayPilot):
    yield NavigationManeuver(url='https://accounts.google.com/signin/v2/identifier?service=androiddeveloper&passive=true&continue=https%3A%2F%2Fplay.google.com%2Fconsole%2Fdeveloper%2F')
    yield SignInManeuver()

    # This interact halts the program so that 2-factor auth can be used to sign in
    # continue after successfully using the Gmail app to sign in, or after the following manual steps are completed
    yield InteractManeuver()

    # ----- comment this block out if these things should be done manually ----------
    company_name = pilot.config['company_name']
    yield ClickElementManeuver(
      instruction=f'click the company name after signing in: {company_name}',
      seeker=lambda p: p.soup.find('div', {'class': 'business-name'}, text=re.compile(company_name))
    )
    sleep(5)
    yield OpenClassicPlayConsoleManeuver()
    # ----- comment this block out if these things should be done manually ----------

    # If the OpenClassicPlayConsole isn't working, or is not to be used, these things need to be done manually:
    # 1. click on the "Use classic Play Console" button
    # 2. copy the url in the new browser window
    # 3. close the new browser window and paste in the copied link in the original browser window
    # 4. re-select the client account
    # 5. enter "C" for "Continue" in the console

    yield ClickXPathSequenceManeuver(xpaths=[
      # f"//a/descendant::span[text()='{pilot.company_name}']",
      f"//a/descendant::div[text()='{pilot.app_id}']",
      "//button/descendant::span[text()='User acquisition']",
      "//a/descendant::span[text()='Acquisition reports']",
    ])

    sleep(5)
    date_keys = {'apcs', 'apce'}
    url_parts = [p.split('=') for p in pilot.browser.current_url.split('&')]

    now = datetime.datetime.utcnow()
    for p in url_parts:
      if p[0] in date_keys:
        p[1] = datetime.datetime.strftime(now, '%Y-%m-%d')

    url_parts.append(['ts', 'FIFTEEN_DAYS'])
    new_url = '&'.join(['='.join(p) for p in url_parts])
    yield NavigationManeuver(url=new_url)

    sleep(10)
    url_parts = [p.split('=') for p in pilot.browser.current_url.split('&')]
    for p in url_parts:
      if p[0] in date_keys:
        url_date = p[1]

    last_date_available = datetime.datetime.strptime(url_date, '%Y-%m-%d')
    data_frame = pd.DataFrame()
    date_range = [last_date_available - datetime.timedelta(days=d) for d in range(pilot.days_back)]

    data_frame = (yield ScrapeDateRangeManeuver(date_range=date_range)).deploy()
    data_frame['app_id'] = pilot.app_id
    data_frame.reset_index(drop=True, inplace=True)
    data_frame.to_csv(str(pilot.download_path / 'GooglePlayScraper.csv'))
    pilot.ordnance = data_frame

    def month_delta(date, delta):
      m, y = (date.month+delta) % 12, date.year + ((date.month)+delta-1) // 12
      if not m: m = 12
      d = min(date.day, calendar.monthrange(y, m)[1])
      return date.replace(day=d,month=m, year=y)

    yield DownloadBulkExportManeuver(
      download_dates=[
        last_date_available,
        month_delta(last_date_available, -1),
      ]
    )

    processor = (yield ProcessGooglePlayDataManeuver()).deploy()
    yield SendDataToSlackManeuver(
      data_path=processor.processed_data_path,
      min_date=processor.min_processed_date,
      max_date=processor.max_processed_date
    )

if __name__ == '__main__':
  enqueue_maneuver(GooglePlayManeuver())
else:
  enqueue_maneuver(DownloadBulkExportManeuver())
