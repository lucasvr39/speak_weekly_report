# Speak weekly report


Nothing more than a full script to run as a cron job to feed an azure storage container with weekly reports that I should deploy to run in the weekends

# Dependencies
The script is written in Python 3. Dependencies can be installed using pip with: 

```
pip install -r requirements.txt
```

### Notes

1. I recommend creating simple virtual environments to run it. There is nothing special with this script, therefore you don't need anything more than that.
2. Using buffer to prevent to write data files but logs in the VPS disk.