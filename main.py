import argparse
import sys
import re
import csv
import subprocess
# pip install pandas
import pandas

# pip install python-dotenv
from dotenv import load_dotenv
import os
# import to get user who ran script
import getpass
# python -m pip install pymongo
import pymongo

from datetime import date

parser = argparse.ArgumentParser()

parser.add_argument("--files", dest="workFiles", nargs="+", help="Baselight/Flame files to process")
parser.add_argument("--xytech", dest="workOrder", help="Xytech file to process")
parser.add_argument("--verbose", action="store_true", help="flag to show db call console output")
parser.add_argument("--output", dest="output", action="store_true", help="flag for CSV(true) or DB(false) output")
parser.add_argument("--process", help="pipe video file")
args = parser.parse_args()

# check if base/flame file have been passed, else exit
if args.workFiles is None:
    print("No Baselight/Flame files selected")
    sys.exit()

# check if flame file has been passed, else exit
if args.workOrder is None:
    print("No Xytech files selected")
    sys.exit()

i = 0
j = 0

# save parsed machine, name of user on file, date
file_names = []
machine = []
file_user = []
file_date = []
while i < len(args.workFiles):
    file_names.append(args.workFiles[i])
    file_names[i] = re.sub('.txt', '', file_names[i])
    file_names[i] = re.split('_', file_names[i])
    machine.append(file_names[i][0])
    file_user.append(file_names[i][1])
    file_date.append(file_names[i][2])
    i += 1
else:
    i = 0

# save user who ran the script (me)
user = getpass.getuser()

# save current date
curr_date = date.today().strftime("%Y-%m-%d")

# pass baselight and flame files
base_flame_file = ['']
for file in args.workFiles:
    with open(file, 'r') as files:
        base_flame_file[0] += files.read()
base_flame_file = base_flame_file[0].splitlines()

# pass index for database stuff
db_flame = []
for file in args.workFiles:
    with open(file, 'r') as files:
        db_flame.append(files.read())
db_flame.pop(0)

db_flame_join = ['']
while i < len(db_flame):
    db_flame_join[0] += db_flame[i]
    i += 1
else:
    i = 0
db_flame_join = db_flame_join[0].splitlines()

temp_flame = []
for line in db_flame_join:
    split = line.split()
    new_line = " ".join(split[2:])
    temp_flame.append(new_line)
db_flame_join.clear()
db_flame_join = temp_flame.copy()
temp_flame.clear()

flame_int = [list(map(int, s.split())) for s in db_flame_join]

flame_merge_int = []

for sublist in flame_int:
    merged_sublist = []
    for i, num in enumerate(sublist):
        if i == 0 or num != sublist[i-1] + 1:
            if merged_sublist:
                flame_int.append(merged_sublist)
                merged_sublist = []
        merged_sublist.append(num)
    if merged_sublist:
        flame_merge_int.append(merged_sublist)

i = 0
flame_index = len(flame_merge_int)

# Remove any <err> or <null> in base/flame files
while i < len(base_flame_file):
    base_flame_file[i] = re.sub(" <err>| <null>", "", base_flame_file[i])
    i += 1
else:
    i = 0

# Combine flame files storage with location
for x in range(len(base_flame_file)):
    base_flame_file[x] = base_flame_file[x].replace(' A', '/A')

# split base/flame further 1)with only the location, 2) with only the ranges
base_flame_location = []
base_flame_ranges = []

while i < len(base_flame_file):
    base_flame_file[i] = base_flame_file[i].split(" ", 1)

    base_flame_location.append(base_flame_file[i][0])
    base_flame_location[i] = base_flame_location[i].split("/")[-2:]
    base_flame_location[i] = "/".join(base_flame_location[i])

    base_flame_ranges.append(base_flame_file[i][1])
    base_flame_ranges[i] = base_flame_ranges[i].split(" ")
    i += 1
else:
    i = 0

while i < len(base_flame_ranges):
    while j < len(base_flame_ranges[i]):
        base_flame_ranges[i][j] = int(base_flame_ranges[i][j])
        j += 1
    j = 0
    i += 1
else:
    i = 0
    j = 0


# pass xytech files
xytech_file = []
xytech_file_location = []
xytech_file_info = []
with open(args.workOrder, 'r') as file:
    xytech_file = file.read().splitlines()

# split xytech file with 1) locations and 2) producer, operator, job, notes
while i < len(xytech_file):
    if "/" in xytech_file[i]:
        xytech_file_location.append(xytech_file[i])
    elif "Producer" in xytech_file[i] or "Operator" in xytech_file[i] or "Job" in xytech_file[i]:
        xytech_file[i] = re.sub("Producer: |Operator: |Job: ", "", xytech_file[i])
        xytech_file_info.append(xytech_file[i])
    elif "Notes" in xytech_file[i]:
        xytech_file_info.append(xytech_file[i+1])
    i += 1
else:
    i = 0

xytech_file_location_short = []
while i < len(xytech_file_location):
    xytech_file_location_short.append(xytech_file_location[i].split("/", 5))
    i += 1
else:
    i = 0

# Match and Replace
while i < len(base_flame_location):
    while j < len(xytech_file_location):
        if base_flame_location[i] == xytech_file_location_short[j][5]:
            base_flame_location[i] = re.sub(base_flame_location[i], xytech_file_location[j], base_flame_location[i])
        j += 1
    j = 0
    i += 1
else:
    j = 0
    i = 0

# fix the ranges
base_flame_ranges_con = []
temp_ranges = []
for i in range(len(base_flame_ranges)):
    base_flame_ranges_con.append([])
    for j in range(len(base_flame_ranges[i])-1):
        if base_flame_ranges[i][j] + 1 == base_flame_ranges[i][j+1]:
            temp_ranges.append(base_flame_ranges[i][j])
        else:
            temp_ranges.append(base_flame_ranges[i][j])
            if len(temp_ranges) > 1:
                base_flame_ranges_con[i].append(str(temp_ranges[0]) + "-" + str(temp_ranges[-1]))
                temp_ranges.clear()
            else:
                base_flame_ranges_con[i].append(str(temp_ranges[0]))
                temp_ranges.clear()
    if len(temp_ranges) > 0:
        temp_ranges.append(base_flame_ranges[i][-1])
        base_flame_ranges_con[i].append(str(temp_ranges[0]) + "-" + str(temp_ranges[-1]))
        temp_ranges.clear()
    else:
        base_flame_ranges_con[i].append(str(base_flame_ranges[i][-1]))
else:
    i = 0
    j = 0

# combine the directories and ranges
combined = []
while i < len(base_flame_location):
    while j < len(base_flame_ranges_con[i]):
        combined.append(base_flame_location[i] + " " + base_flame_ranges_con[i][j])
        j += 1
    i += 1
    j = 0
else:
    j = 0
    i = 0

# split for csv export
combined_location = []
combined_ranges = []

for elem in combined:
    parts = elem.split(" ")
    combined_location.append(parts[0])
    combined_ranges.append(" ".join(parts[1:]))

# load environment variables from .env
load_dotenv()

# retrieve connection screen from .env
db_string = os.getenv("DB_STRING")

# connect to db
client = pymongo.MongoClient(db_string)

# create database
mydb = client["Proj3DB"]

# create collections
file_submission = mydb["fileSub"]
file_metadata = mydb["fileMD"]

# output to csv or DB (output flag)
if args.output:
    print("CSV File output\n")

    # export to csv
    with open("proj3.csv", "w", newline="") as csvfile:
        fieldnames = ["Producer", "Operator", "Job", "Notes"]
        thewriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
        thewriter.writeheader()
        thewriter.writerow({"Producer": xytech_file_info[0],
                            "Operator": xytech_file_info[1],
                            "Job": xytech_file_info[2],
                            "Notes": xytech_file_info[3]})

        blankname = ["Blank"]
        newWrite = csv.DictWriter(csvfile, fieldnames=blankname)
        newWrite.writerow({"Blank": " "})

        splitname = ["Left", "Right"]
        lastWrite = csv.DictWriter(csvfile, fieldnames=splitname)

        k = 0
        for combined in combined_location:
            lastWrite.writerow({"Left": combined_location[k], "Right": combined_ranges[k]})
            k += 1

else:
    print("BD output")

    # insert into collections

    insert_sub = {"User that ran script": user, "Machine": machine, "Name of User on file": file_user,
                  "Date of file": file_date, "Submitted date": curr_date}
    insert_in_sub = file_submission.insert_one(insert_sub)
    print("Data has been inserted into collection 1\n",
          file_submission.find_one({"_id": insert_in_sub.inserted_id}), "\n")

    insert_meta = {"Name of user on file": file_user, "Date of file": file_date,
                   "Location": combined_location, "Frame/Ranges": combined_ranges}
    insert_in_meta = file_metadata.insert_one(insert_meta)
    print("Data has been inserted into collection 2\n",
          file_metadata.find_one({"_id": insert_in_meta.inserted_id}))

# print database calls to console
if args.verbose:

    print("DATABASE CALLS\n")

    print("All work done by user TDanza:")
    db_call_one = file_metadata.find({"Name of user on file": ["TDanza"]})
    for call in db_call_one:
        for loc, fr in zip(call["Location"], call["Frame/Ranges"]):
            print(loc, fr)

    print("\nAll work done before 3-25-2023 date on a Flame:")
    # Find the indexes of all flame
    indexes = []
    for i, machine in enumerate(file_submission.find_one()["Machine"]):
        if machine == "Flame":
            indexes.append(i)

    # Save the corresponding user and dates from flame
    for index in indexes:
        user_sub_two = file_submission.find_one()['Name of User on file'][index]
        date_sub_two = file_submission.find_one()['Date of file'][index]
    db_call_two_md = file_metadata.find_one({
        "Name of user on file": user_sub_two,
        "Date of file": date_sub_two
    })

    if db_call_two_md:
        locations = db_call_two_md.get('Location', [])
        frames_ranges = db_call_two_md.get('Frame/Ranges', [])
        last_locations = locations[-flame_index:]
        last_frames = frames_ranges[-flame_index:]

    for location, frame in zip(last_locations, last_frames):
        print(location, frame)

    print("\nWork done on hpsans13 on date 3-26-2023:")

    loc_name = re.compile(f".*{'hpsans13'}.*", re.IGNORECASE)

    db_call_three_md = file_metadata.find({
        "Location": {"$regex": loc_name},
        "Date of file": "20230326"
    })

    if any(db_call_three_md):
        for doc in db_call_three_md:
            print(doc["Location"])
    else:
        print("NONE")

    print("\nName of all Autodesk Flame users:")

    db_call_four_sub = file_submission.find({
        "Machine": "Flame",
    })

    flame_users = []
    for doc in db_call_four_sub:
        for i, machine in enumerate(doc["Machine"]):
            if machine == "Flame":
                flame_users.append(doc["Name of User on file"][i])
    print(flame_users)

if args.process:

    # using ffprobe to get the length of the video
    video_length = float(subprocess.check_output(["ffprobe", "-v", "error", "-show_entries",
                                                    "format=duration", "-of",
                                                  "default=noprint_wrappers=1:nokey=1", args.process]))

    # convert length to 60 fps, this gives me an FPS of 5977
    framesPerSec = int(video_length * 60)

    print(f"video length is {video_length} seconds. FPS: {framesPerSec}")

    # access DB from proj 2 and store ranges that fall under 5977
    fps_list = []
    for document in file_metadata.find({}, {"Frame/Ranges": 1}):
        frames_ranges_3 = document.get("Frame/Ranges", [])

        for frame_range in frames_ranges_3:
            range_values_3 = frame_range.split('-')
            start_value_3 = int(range_values_3[0])
            end_value_3 = int(range_values_3[1]) if len(range_values_3) > 1 else start_value_3

            # get ranges that fall under the videos total frames
            if start_value_3 <= framesPerSec or end_value_3 <= framesPerSec:
                fps_list.append(frame_range)


    print(f"\n{fps_list}\n")

    print(len(fps_list))



    # remove any single frames and keep ranges
    fps_list_ranges = []
    for fps in fps_list:
        if "-" in fps:
            fps_list_ranges.append(fps)

    print(f"\n{fps_list_ranges}\n")
    print(len(fps_list_ranges))

    # range_fps = []
    # for fps in fixed_fps:
    #     if "-" in fps:
    #         range_fps.append(fps)
    #
    # range_fps_c = range_fps.copy()
    # print(f"\n{range_fps}\n")
    #
    # range_fps_L = []
    # range_fps_R = []
    # while i < len(range_fps):
    #     range_fps[i] = re.split('-', range_fps[i])
    #     range_fps_L.append(int(range_fps[i][0]))
    #     range_fps_R.append(int(range_fps[i][1]))
    #     i += 1
    # else:
    #     i = 0
    #
    # fps_L = []
    # fps_R = []
    #
    # for frames in range_fps_L:
    #     fps_L.append(frames/60)
    #
    # for frames in range_fps_R:
    #     fps_R.append(frames/60)
    #
    # timeCode = []
    #
    # while i < len(fps_L):
    #     timeCL = pandas.to_datetime(fps_L[i], unit='s').strftime("%H:%M:%S:" + str(range_fps_L[i]))
    #     timeCR = pandas.to_datetime(fps_R[i], unit='s').strftime("%H:%M:%S:" + str(range_fps_R[i]))
    #     timeCode.append(timeCL + "/" + timeCR)
    #     i += 1
    # else:
    #     i = 0
    #
    # for x in range(len(timeCode)):
    #     print(timeCode[x])
    #
    # print(f"\n{range_fps_c}\n")




    #python main.py --files Baselight_THolland_20230327.txt Flame_DFlowers_20230327.txt --xytech Xytech_20230327.txt --output --process .\twitch_nft_demo.mp4


