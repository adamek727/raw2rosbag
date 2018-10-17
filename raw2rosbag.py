import argparse

import rospy
import rosbag

import csv
import matplotlib.pyplot as plt
from tqdm import tqdm

import cv2
import cv_bridge


class BagWriter:

    def __init__(self, path, name, calib_folder):

        self.path = path + '/'
        self.name = name
        self.calib_dir = calib_folder

        #compression = rosbag.Compression.NONE   # 660MB / 100 imgs
        #compression = rosbag.Compression.BZ2    # 107MB / 100 imgs
        compression = rosbag.Compression.LZ4     # 273MB / 100 imgs

        self.bag = rosbag.Bag( self.path + self.name + '_syn' + '.bag' , 'w', compression=compression)

    def __del__(self):

        print(" * Bag Overview:")
        print(self.bag)
        self.bag.close()


    def write_camera(self, folder, topic, frame):

        print(' Writing camera: ' + topic)

        timestamps = self.read_csv(self.path + self.name + '/' + folder + '/' + 'timestamps.txt')
        bridge = cv_bridge.CvBridge()

        for row in tqdm(timestamps):

            filename = row[0] + '.jpeg'
            timestamp = row[1]

            image_path = self.path + self.name + '/' + folder + '/' + filename
            img = cv2.imread(image_path)

            encoding = "bgr8"
            image_message = bridge.cv2_to_imgmsg(img, encoding=encoding)
            image_message.header.frame_id = frame

            image_message.header.stamp.secs = int(float(timestamp))
            image_message.header.stamp.nsecs = (float(timestamp) - image_message.header.stamp.secs ) * 1000000000

            self.bag.write(topic + '/image_raw', image_message, t=image_message.header.stamp)



    def write_lidar(self, folder, topic, frame):

        print(' Writing lidar: ' + topic)
        pass


    def write_imu(self, folder, topic, frame):

        print(' Writing imu: ' + topic)
        pass


    def write_gnss(self, folder, topic, frame):

        print(' Writing gnss: ' + topic)
        pass


    def write_tf(self):
        pass


    def read_csv(self, path):

        file = []
        with open(path, 'r') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            for row in spamreader:
                file.append(row)

        return file[1:]  # ignore file header

def print_help():
    print('-'*50)
    print('Help')
    print(' Usage: raw2rosbag.py -p <folder> -n <name>')
    print(' Example: raw2rosbag.py -p /home/user/my_rad_dataset -n recording_1')
    print('-'*50)


def main():

    parser = argparse.ArgumentParser(description="Convert RAW data into rosbag file.")
    parser.add_argument("-p", "--path", help="system path to folder, where all dataset recording are stored")
    parser.add_argument("-n", "--name", help="specific recording folder name")
    args = parser.parse_args()

    if args.name == None:
        print('! Error: missing name of the record')
        print_help()
        return -1

    if args.path == None:
        print('! Error: missing path to the dataset folder')
        print_help()
        return -1

    topic_offset = ''
    sensor_types = ['camera', 'lidar', 'imu', 'gnss']
    calib_folder = 'calibration'

    sensor2topics = {
        'camera': {
            'camera_front_left' : [topic_offset + '/camera_left', 'camera_left'],
            'camera_front_right': [topic_offset + '/camera_right', 'camera_right']
        },
        'lidar': {
            'lidar_left': [topic_offset + '/lidar_left', 'lidar_left'],
            'lidar_right': [topic_offset + '/lidar_right', 'lidar_right']
        },
        'imu': {
            'gps_imu': [topic_offset + '/imu', 'frame_center']
        },
        'gnss': {
            'gps_imu': [topic_offset + '/gnss', 'gnss_rare']
        }
    }

    bagHnadler = BagWriter(args.path, args.name, calib_folder)

    for type in sensor2topics.keys():
        if type in sensor_types:
            for sensor in sensor2topics[type]:

                folder = sensor
                topic = sensor2topics[type][sensor][0]
                frame = sensor2topics[type][sensor][1]

                if type == sensor_types[0]:
                    bagHnadler.write_camera(folder, topic, frame)

                elif type == sensor_types[1]:
                    bagHnadler.write_lidar(folder, topic, frame)

                elif type == sensor_types[2]:
                    bagHnadler.write_imu(folder, topic, frame)

                elif type == sensor_types[3]:
                    bagHnadler.write_gnss(folder, topic, frame)


if __name__ == '__main__':
    main()