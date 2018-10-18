import argparse
from BagWriter import BagWriter
import yaml

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
    transforms_file = 'frames.yaml'

    sensor2topics = {
        'camera': {
            'camera_front_left' : [topic_offset + '/camera_left', 'camera_left', '.jpeg'],
            'camera_front_right': [topic_offset + '/camera_right', 'camera_right', '.jpeg']
        },
        'lidar': {
            'lidar_left': [topic_offset + '/lidar_left', 'velodyne_left', '.bin'],
            'lidar_right': [topic_offset + '/lidar_right', 'velodyne_right', '.bin']
        },
        'imu': {
            'gps_imu': [topic_offset + '/imu', 'frame_center']
        },
        'gnss': {
            'gps_imu': [topic_offset + '/gnss', 'gnss_antena_rear']
        }
    }

    bagHnadler = BagWriter(args.path, args.name, calib_folder)

    stream = open(args.path + '/'+ args.name + '/' + calib_folder + '/' + transforms_file, "r")
    transforms = yaml.load_all(stream)
    for content in transforms:
        for key, val in content.items():
            if(key == 'transforms'):
                bagHnadler.write_tf(val)

                #for transform in val:
                #    print(transform['label'])
                #    print(transform['parent'])
                #    print(transform['tran'])
                #    print(transform['rot'])
                #print('\n\n')


    for type in sensor2topics.keys():
        if type in sensor_types:
            for sensor in sensor2topics[type]:

                folder = sensor
                topic = sensor2topics[type][sensor][0]
                frame = sensor2topics[type][sensor][1]

                if type == sensor_types[0]:
                    ext = sensor2topics[type][sensor][2]
                    bagHnadler.write_camera(folder, topic, frame, ext)

                elif type == sensor_types[1]:
                    ext = sensor2topics[type][sensor][2]
                    bagHnadler.write_lidar(folder, topic, frame, ext)

                elif type == sensor_types[2]:
                    bagHnadler.write_imu(folder, topic, frame)

                elif type == sensor_types[3]:
                    bagHnadler.write_gnss(folder, topic, frame)


if __name__ == '__main__':
    main()