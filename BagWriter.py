import rospy
import rosbag

import numpy as np

import csv
import matplotlib.pyplot as plt
from tqdm import tqdm

import cv2
import cv_bridge
import pcl_ros

import sensor_msgs.point_cloud2 as pc2
from sensor_msgs.msg import CameraInfo
from sensor_msgs.msg import Imu
from sensor_msgs.msg import PointCloud2

from std_msgs.msg import Header

from geometry_msgs.msg import PoseStamped


class BagWriter:

    def __init__(self, path, name, calib_folder):

        self.path = path + '/'
        self.name = name
        self.calib_dir = calib_folder

        compression = rosbag.Compression.NONE   # 660MB / 100 imgs
        #compression = rosbag.Compression.BZ2    # 107MB / 100 imgs
        #compression = rosbag.Compression.LZ4     # 273MB / 100 imgs

        self.bag = rosbag.Bag( self.path + self.name + '_syn' + '.bag' , 'w', compression=compression)

    def __del__(self):

        print(" * Bag Overview:")
        print(self.bag)
        self.bag.close()


    def write_camera(self, folder, topic, frame, ext):

        print(' Writing camera: ' + topic)

        records = self.read_csv(self.path + self.name + '/' + folder + '/' + 'timestamps.txt')
        bridge = cv_bridge.CvBridge()

        seq = 0
        for row in tqdm(records):

            filename = row[0] + ext
            timestamp = row[1]

            image_path = self.path + self.name + '/' + folder + '/' + filename
            img = cv2.imread(image_path)

            encoding = "bgr8"
            image_message = bridge.cv2_to_imgmsg(img, encoding=encoding)

            image_message.header.frame_id = frame
            image_message.header.stamp = self.timestamp_to_stamp(timestamp)
            image_message.header.seq = seq
            seq +=1

            self.bag.write(topic + '/image_raw', image_message, t=image_message.header.stamp)


            camera_info = CameraInfo()
            camera_info.header = image_message.header
            camera_info.height = img.shape[0]
            camera_info.width = img.shape[1]



    def write_lidar(self, folder, topic, frame, ext):

        print(' Writing lidar: ' + topic)

        records = self.read_csv(self.path + self.name + '/' + folder + '/' + 'timestamps.txt')

        seq = 0
        for row in tqdm(records):

            filename = row[0] + ext
            timestamp = row[1]

            scan = np.fromfile(self.path + self.name + '/' + folder + '/' + filename, dtype=np.float32)
            scan = scan.reshape(-1, 4)

            header = Header()
            header.frame_id = frame
            #header.stamp = self.timestamp_to_stamp(timestamp)
            header.stamp.secs = seq / 10
            header.stamp.nsecs = (seq % 10) * 100000000
            seq += 1

            fields = [pc2.PointField('x', 0,  pc2.PointField.FLOAT32, 1),
                      pc2.PointField('y', 4,  pc2.PointField.FLOAT32, 1),
                      pc2.PointField('z', 8,  pc2.PointField.FLOAT32, 1),
                      pc2.PointField('i', 12, pc2.PointField.FLOAT32, 1)]

            pc_message = pc2.create_cloud(header, fields, scan)

            self.bag.write(topic, pc_message, t=pc_message.header.stamp)




    def write_imu(self, folder, topic, frame):

        print(' Writing imu: ' + topic)

        records = self.read_csv(self.path + self.name + '/' + folder + '/' + 'imu.txt')

        seq = 0
        for row in tqdm(records):

            timestamp = row[1]
            imu = Imu()

            imu.header.frame_id = frame
            imu.header.stamp = self.timestamp_to_stamp(timestamp)
            imu.header.seq = seq
            seq +=1

            imu.orientation.x = float(row[2])
            imu.orientation.y = float(row[3])
            imu.orientation.z = float(row[4])
            imu.orientation.w = float(row[5])
            imu.linear_acceleration.x = float(row[6])
            imu.linear_acceleration.y = float(row[7])
            imu.linear_acceleration.z = float(row[8])
            imu.angular_velocity.x = float(0)
            imu.angular_velocity.y = float(0)
            imu.angular_velocity.z = float(0)

            self.bag.write(topic, imu, t=imu.header.stamp)


    def write_gnss(self, folder, topic, frame):

        print(' Writing gnss: ' + topic)

        records = self.read_csv(self.path + self.name + '/' + folder + '/' + 'gnss.txt')

        seq = 0
        for row in tqdm(records):

            timestamp = row[1]
            pose = PoseStamped()

            pose.header.frame_id = frame
            pose.header.stamp = self.timestamp_to_stamp(timestamp)
            pose.header.seq = seq
            seq += 1

            pose.pose.position.x = float(row[2])
            pose.pose.position.y = float(row[3])
            pose.pose.position.z = float(row[4])

            pose.pose.orientation.z = float(row[5])
            pose.pose.orientation.z = float(row[6])
            pose.pose.orientation.z = float(row[7])
            pose.pose.orientation.z = float(row[8])

            self.bag.write(topic, pose, t=pose.header.stamp)

    def write_tf(self):
        pass


    def read_csv(self, path):

        file = []
        with open(path, 'r') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',')
            for row in spamreader:
                file.append(row)

        return file[1:]  # ignore file header


    def timestamp_to_stamp(self, timestamp):

        stamp = rospy.Time()
        stamp.secs = int(float(timestamp))
        stamp.nsecs = (float(timestamp) - stamp.secs) * 1000000000

        return stamp
