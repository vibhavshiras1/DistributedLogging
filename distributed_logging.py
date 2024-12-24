import random
import time
import paramiko
import os
import threading
import datetime

class DistributedLogging:

    def __init__(self, server, interval, timeout, shared_max, file_size):
        self.server = server
        self.interval = interval
        self.root_dir = "/tmp/stats_logging/"
        self.timeout = timeout
        self.file_size = file_size

        self.global_max = -1
        self.shared_max = shared_max
        self.lock = threading.Lock()

        self.establish_ssh_connection()
        self.create_required_dirs()

    def tearDown(self):
        self.print_log("Removing the root directory")
        self.ssh.exec_command("rm -rf {}".format(self.root_dir))

        self.print_log("Closing SSH connection")
        self.ssh.close()

    def print_log(self, message):
        print('{} - {} - {}'.format(datetime.datetime.now(),self.server, message))

    def create_required_dirs(self):

        cmd = "mkdir -p {}".format(self.root_dir)
        stdin, stdout, stderr = self.ssh.exec_command(cmd)
        self.print_log("Created root dir: {}".format(self.root_dir))

    def establish_ssh_connection(self):

        self.print_log("Establishing SSH connection")
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(self.server, username="root", password="couchbase")

    def generate_stats_interval(self):
        stat_counter = 0
        file_counter = 1

        file_name = "stats" + str(file_counter) + ".log"
        file_name = os.path.join(self.root_dir, file_name)
        self.print_log("Generating stats to file: {}".format(file_name))

        self.end_time = time.time() + self.timeout
        while time.time() < self.end_time:
            num = random.randint(100000, 999999)
            insert_cmd = "echo {} >> {}".format(num, file_name)
            self.print_log("Inserting new stat record")
            stdin, stdout, stderr = self.ssh.exec_command(insert_cmd)
            stat_counter += 1
            if stat_counter >= self.file_size:
                file_counter += 1
                stat_counter = 0
                file_name = "stats" + str(file_counter) + ".log"
                file_name = os.path.join(self.root_dir, file_name)
                self.print_log("Generating stats to file: {}".format(file_name))
            time.sleep(self.interval)

        self.check()

    def check(self):

        stdin, stdout, stderr = self.ssh.exec_command("ls {}".format(self.root_dir))
        stats_files = stdout.readlines()
        total_records = 0

        for stat_file in stats_files:
            stat_file = stat_file.strip()
            cmd = "wc -l {}".format(os.path.join(self.root_dir, stat_file))
            stdin, stdout, stderr = self.ssh.exec_command(cmd)
            output = stdout.readlines()
            total_records += int(output[0].split()[0])

        self.print_log("Total stat records = {}".format(total_records))

    def find_max(self, file):
        full_file_path = os.path.join(self.root_dir, file)
        cat_cmd = "cat {}".format(full_file_path)
        stdin, stdout, stderr = self.ssh.exec_command(cat_cmd)

        local_max = -1
        num_list = stdout.readlines()
        for num in num_list:
            num = int(num.strip())
            local_max = max(local_max, num)

        self.print_log("File: {}, Max: {}".format(file, local_max))

        # Change global_max if local_max > global_max
        # Acquire a lock while changing the variable
        with self.lock:
            self.global_max = max(local_max, self.global_max)

    def process_stats(self):
        self.global_max = -1
        process_th_array = list()

        stdin, stdout, stderr = self.ssh.exec_command("ls {}".format(self.root_dir))
        self.stats_files = stdout.readlines()
        self.print_log("List of Stats files = {}".format(self.stats_files))

        self.print_log("Processing Stat files")
        for stat_file in self.stats_files:
            stat_file = stat_file.strip()
            th = threading.Thread(target=self.find_max, args=[stat_file])
            th.start()
            process_th_array.append(th)

        for th in process_th_array:
            th.join()

        self.print_log("Global max = {}".format(self.global_max))

        with self.lock:
            self.shared_max[0] = max(self.shared_max[0], self.global_max)


if __name__ == "__main__":

    servers = ["172.23.217.173", "172.23.217.174"]
    th_array = list()
    obj_list = list()
    shared_global_max = [-1]

    for server in servers:
        obj = DistributedLogging(server=server, interval=0.5, timeout=30, file_size=20, shared_max=shared_global_max)
        th = threading.Thread(target=obj.generate_stats_interval)
        th.start()
        th_array.append(th)
        obj_list.append(obj)

    for th in th_array:
        th.join()

    th_array = list()
    for obj in obj_list:
        th = threading.Thread(target=obj.process_stats)
        th.start()
        th_array.append(th)

    for th in th_array:
        th.join()

    print("Global max across all nodes = {}".format(shared_global_max[0]))

    for obj in obj_list:
        obj.tearDown()