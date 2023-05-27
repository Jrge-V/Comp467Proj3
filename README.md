# Project : Marks Automation Script


- An automation script using Python, that automates 4 manual positions in the matter of seconds
- Python code reads proprietary data from Baselight and Flame machines to calculate filesystem locations of frames
- All requests saved to MongoDB and can used for data analysis and worker effeciency
- Exports are basic CSV files to XLS files with timecode and thumbnail preview that can uploaded to Frame.IO and/or Shotgrid

### Example run
`python main.py --files Baselight_THolland_20230327.txt Flame_DFlowers_20230327.txt --xytech Xytech_20230327.txt --output xls  --process .\twitch_nft_demo.mp4
`