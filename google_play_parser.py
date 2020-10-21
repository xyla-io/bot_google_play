import re
import pandas as pd

from raspador import OrdnanceParser, XPath

class DateNotAvailableParser(OrdnanceParser[XPath]):
  def parse(self):
    element = self.soup.find(text=re.compile(r'Date not available'))
    self.ordnance = self.xpath_for_element(element=element) if element else None
    return self

class AcquisitionReportParser(OrdnanceParser[pd.DataFrame]):
  def parse(self):
    listing_elements = self.soup.find_all('div', {'title': re.compile(r'Number of people that have never installed your app who visited your store listing')})
    first_time_installer_elements = self.soup.find_all('div', {'title': re.compile(r'Number of users who installed your app for the first time.')})

    acquisition_data = [{
      'acquisition_channel': 'search',
      'store_listing_visitors': listing_elements[2].text,
      'first_time_installers': first_time_installer_elements[2].text,
    }, {
      'acquisition_channel': 'explore',
      'store_listing_visitors': listing_elements[3].text,
      'first_time_installers': first_time_installer_elements[3].text,
    }]
    df = pd.DataFrame(acquisition_data)
    df.store_listing_visitors = df.store_listing_visitors.str.replace(',', '').astype(int)
    df.first_time_installers = df.first_time_installers.str.replace(',', '').astype(int)
    self.ordnance = df
    return self
