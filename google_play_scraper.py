import os

from pathlib import Path
from raspador import Raspador, ReportRaspador, ScriptManeuver
from typing import Dict
from .google_play_pilot import GooglePlayPilot

class GooglePlayBot(ReportRaspador):
  def scrape(self):
    maneuver = ScriptManeuver(script_path=str(Path(__file__).parent / 'google_play_maneuver.py'))
    pilot = GooglePlayPilot(config=self.configuration, browser=self.browser, user=self.user)
    
    self.fly(pilot=pilot, maneuver=maneuver)
    self.load(ordnance=pilot.deploy())

    super().scrape()