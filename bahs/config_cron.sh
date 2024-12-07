#!/bin/bash

if ! command -v cron &> /dev/null; then
    sudo apt-get update
    sudo apt-get install -y cron
fi

sudo systemctl start cron
sudo systemctl enable cron

(crontab -l 2>/dev/null; echo "*/32 * * * * /usr/bin/python3 /home/ubuntu/horus/simulador-de-dados/aws_data_generation/app.py >> /home/ubuntu/horus/log.txt 2>&1") | crontab -
(crontab -l 2>/dev/null; echo "*/33 * * * * /usr/bin/python3 /home/ubuntu/horus/simulador-de-dados/aws_data_generation/add_data_in_database.py >> /home/ubuntu/horus/log_db.txt 2>&1") | crontab -
