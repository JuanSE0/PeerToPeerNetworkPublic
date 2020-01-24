#!/usr/bin/python

import socket
import time
import threading
import json
import sys
from Queue import Queue
from concurrent import futures


class TrackerOperations(threading.Thread):
    def __init__(self, threadid, name):
        """
        Constructor used to initialize class object.

        """
        threading.Thread.__init__(self)
        self.threadID = threadid
        self.name = name
        self.portPeers = {}
        self.fileIndex = {}
        #self.fileChunks = {}
        self.hash_table_replica_files = {}
        self.peerFiles = {}
        self.peerID = []
        self.peerFilesSize = {}
        self.listener_queue = Queue()

    def trackerListener(self):

        try:
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_host = socket.gethostname()
            server_socket.bind((server_host, 0))

            f = open("port.txt", 'w')
            f.write(str(server_socket.getsockname()[1]))
            f.close()

            server_socket.listen(10)
            while True:
                conn, addr = server_socket.accept()
                self.listener_queue.put((conn, addr))
        except Exception as e:
            print "Server Listener on port Failed: %s" % e
            sys.exit(1)

    '''
    We will use this method whenever a new peer enters the system.
    We will store the name of the address and port of the peer as
    well as the name and size of each file
    '''

    def registry(self, addr, files, filesS, peer_port):

        try:
            self.portPeers[peer_port] = addr[0]
            peerID = addr[0] + ":" + str(peer_port)
            self.peerFiles[peerID] = files
            self.peerFilesSize[peerID] = filesS
            self.peerID.append(peerID)

            for f in files:
                if self.fileIndex.has_key(f):
                    self.fileIndex[f].append(peerID)
                else:
                    self.fileIndex[f] = [peerID]

            print "PEER %s CONNECT: OFFERS" % (len(self.peerFiles) - 1), \
                len(self.peerFiles[peerID])

            for index in range(0, len(self.peerFiles[peerID])):
                if self.peerFilesSize[peerID][index] % 512 != 0:
                    print index, "  ", self.peerFiles[peerID][index], \
                        (self.peerFilesSize[peerID][index] / 512 + 1)
                else:
                    print index, "  ", self.peerFiles[peerID][index], \
                        self.peerFilesSize[peerID][index] / 512
            return True

        except Exception as e:
            print "Error while registering peer: %s" % e
            return False

    def updateChunk(self, dataReceived):

        try:
            for i in range(1, dataReceived['chunk']+1):
                print "PEER %s ACQUIRED CHUNK %s/%s %s" % (dataReceived['prettyID'], \
                                                i, dataReceived['chunk'], dataReceived['fileName'])

            return True

        except Exception as e:
            print "Error while printing chunks received by peer. Exception: %s" % e
            return False

    def listFileIndexTracker(self):

        try:
            filesList = self.fileIndex.keys()
            return filesList
        except Exception as e:
            print "Listing Files Error, %s" % e

    def search(self, file_name):

        try:
            if self.fileIndex.has_key(file_name):
                peer_list = self.fileIndex[file_name]
            else:
                peer_list = []
            return peer_list
        except Exception as e:
            print "Listing Files Error, %s" % e

    def deregistry(self, peer_data):

        try:
            if self.portPeers.has_key(peer_data['hosting_port']):
                self.portPeers.pop(peer_data['hosting_port'], None)
            if self.peerFiles.has_key(peer_data['peer_id']):
                self.peerFiles.pop(peer_data['peer_id'], None)
            if self.peerFilesSize.has_key(peer_data['peer_id']):
                self.peerFilesSize.pop(peer_data['peer_id'], None)
            for f in peer_data['files']:
                if self.fileIndex.has_key(f):
                    for peer_id in self.fileIndex[f]:
                        if peer_id == peer_data['peer_id']:
                            self.fileIndex[f].remove(peer_id)
                            if len(self.fileIndex[f]) == 0:
                                self.fileIndex.pop(f, None)
            return True
        except Exception as e:
            print "Peer deregistration failure: %s" % e
            return False


    def run(self):

        try:

            listener_thread = threading.Thread(target=self.trackerListener)
            listener_thread.setDaemon(True)
            listener_thread.start()


            while True:
                while not self.listener_queue.empty():
                    with futures.ThreadPoolExecutor(max_workers=8) as executor:
                        conn, addr = self.listener_queue.get()
                        data_received = json.loads(conn.recv(1024))

                        if data_received['command'] == 'register':
                            fut = executor.submit(self.registry, addr,
                                                  data_received['files'],
                                                  data_received['filesSize'],
                                                  data_received['peer_port'])
                            success = fut.result(timeout=None)

                            conn.send(json.dumps([addr[0], success, (len(self.peerFiles)-1)]))

                        elif data_received['command'] == 'updateIndex':
                            '''
                            The peer will send its index after receiving files from any other peer
                            we will then update its index in the tracker with the new files so other peers
                            will be able to request that file from this peer if necessary
                            '''
                            print "Hello there"
                            for file in data_received['files']:
                                # print file

                                if file not in self.peerFiles[data_received['peer_id']]:
                                    print "This is a new file for this peer"
                                    print file
                                    self.peerFiles[data_received['peer_id']].append(file)
                            print "Going out"

                        elif data_received['command'] == 'list':
                            listFiles = []
                            listFiles.append(self.listFileIndexTracker())
                            fut = executor.submit(self.listFileIndexTracker)
                            file_list = fut.result(timeout=None)
                            conn.send(json.dumps(file_list))

                        elif data_received['command'] == 'chunkUpdate':
                            '''
                            The peer will send its index after receiving files from any other peer
                            we will then update its index in the tracker with the new files so other peers
                            will be able to request that file from this peer if necessary
                            '''

                            executor.submit(self.updateChunk,data_received)


                        elif data_received['command'] == 'search':
                            fut = executor.submit(self.search,
                                                  data_received['file_name'])
                            peer_list = fut.result(timeout=None)
                            conn.send(json.dumps(peer_list))

                        elif data_received['command'] == 'deregister':
                            fut = executor.submit(self.deregistry, data_received)
                            success = fut.result(timeout=None)
                            if success:
                                print "PEER %s DISCONNECT: RECEIVED %s" % (data_received['prettyID'],
                                                                    len(data_received['files']))
                                for f in data_received['files']:
                                    print "%s    %s" % (data_received['prettyID'], f)
                                conn.send(json.dumps(success))
                            else:
                                print "deregistration of Peer ID: %s unsuccessful" \
                                      % (data_received['peer_id'])
                                conn.send(json.dumps(success))
                        '''
                        print "hash table: Files || %s" % \
                              self.fileIndex
                        print "hash table: Port-Peers || %s" % \
                              self.portPeers
                        print "hash table: Peer-Files || %s" % \
                              self.peerFiles
                        '''
                        conn.close()
        except Exception as e:
            print "Server Operations error, %s " % e
            sys.exit(1)


if __name__ == '__main__':
    """
    Main method to start deamon threads for listener and operations.
    """
    try:
        operations_thread = TrackerOperations(1, "ServerOperations")
        operations_thread.start()
    except Exception as e:
        sys.exit(1)
