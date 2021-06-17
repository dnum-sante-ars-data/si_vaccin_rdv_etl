@echo off
set domaine=%1
set commande=%2
set date=%3
set verbose=%4
shift
shift
shift
shift
python main.py %domaine% %commande% --date %date% --verbose %verbose%