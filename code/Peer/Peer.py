#!/usr/bin/python

import socket
import time
import threading
import json
import os
import sys
import datetime

sys.path.append('..')
from Queue import Queue
from concurrent import futures

SHAREDDIR = "./Shared"

"""
Constructor for peer operations
"""
class PeerOperations(threading.Thread):
    def __init__(self, threadid, threadName, p, finalTime):
        threading.Thread.__init__(self)
        self.threadID = threadid
        self.threadName = threadName
        self.peer = p
        # In finalTime we will store the time in which we want to derister the peer
        self.finalTime = finalTime
        self.peer_server_listener_queue = Queue()

    """
    This method is used by the peer to listen to the tracker
    """
    def peerTrackerListen(self):

        try:
            peerTrackerSocket = socket.socket(
                socket.AF_INET, socket.SOCK_STREAM)
            peerTrackerSocket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            socketHost = socket.gethostname()
            socketPort = self.peer.hosting_port
            peerTrackerSocket.bind(
                (socketHost, socketPort))
            peerTrackerSocket.listen(10)
            while True:
                conn, addr = peerTrackerSocket.accept()
                self.peer_server_listener_queue.put((conn, addr))
        except Exception as e:
            print "Failed: %s" % e
            sys.exit(1)

    """
    This method purpose is to prepare the peer for uploading to other peers when necessary the other files
    send by other peers
    """
    def peerPrepareUpload(self, socketT, receivedData):
        try:
            f = open(SHAREDDIR + '/' + receivedData['file_name'], 'rb')
            data = f.read()
            f.close()
            socketT.sendall(data)
            socketT.close()
        except Exception as e:
            print "File Upload Error, %s" % e

    """
    This method is used by the peer to send the files to other peers by means of
    replication using a pool of threads.
    """
    def peerPeerPool(self):
        try:
            while True:
                while not self.peer_server_listener_queue.empty():
                    with futures.ThreadPoolExecutor(max_workers=8) as executor:
                        conn, addr = self.peer_server_listener_queue.get()
                        data_received = json.loads(conn.recv(1024))

                        if data_received['command'] == 'obtain_active':
                            executor.submit(
                                self.peerPrepareUpload, conn, data_received)
        except Exception as e:
            print "Peer Server Hosting Error, %s" % e

    def peer_server(self):
        """
        This method is used start peer server listener and peer server
        download deamon thread.
        """
        try:
            listener_thread = threading.Thread(target=self.peerTrackerListen)
            listener_thread.setDaemon(True)

            operations_thread = threading.Thread(target=self.peerPeerPool)
            operations_thread.setDaemon(True)

            listener_thread.start()
            operations_thread.start()

            threads = []
            threads.append(listener_thread)
            threads.append(operations_thread)

            for t in threads:
                t.join()
        except Exception as e:
            print "Peer Server Error, %s" % e
            sys.exit(1)


    def run(self):
        """
        Deamon thread for Peer Server and File Handler.
        """
        if self.threadName == "PeerServer":
            self.peer_server()


class Peer():
    def __init__(self):

        self.server_port = int(sys.argv[2])
        self.fileIndex = []
        self.filesSize = []

    """
    Here we will obtain all the files we currently have in our Shared folder
    """

    def getFileIndexSize(self):
        try:
            for filename in os.listdir(SHAREDDIR):
                self.fileIndex.append(filename)
                self.filesSize.append(os.path.getsize(SHAREDDIR + "/" + filename))
        except Exception as e:
            print "Error: Something went wrong while obtaining the filenames %s" % e

    '''
    This method is used to obtain a port that is not in use
    '''

    def getPortSocket(self):
        try:
            peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            peer_socket.bind(('', 0))
            free_socket = peer_socket.getsockname()[1]
            peer_socket.close()
            return free_socket
        except Exception as e:
            print "Error while obtaining port %s" % e
            sys.exit(1)

    """
    We will inform tracker about our existence and send it our filenames, address and ip
    """

    def regisPeerTracker(self):
        try:
            self.getFileIndexSize()
            availablePort = self.getPortSocket()

            peer_to_server_socket = \
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_to_server_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            '''Connect to tracker using knowing its port and address'''
            peer_to_server_socket.connect(
                (sys.argv[1], self.server_port))

            cmd_issue = {
                'command': 'register',
                'peer_port': availablePort,
                'files': self.fileIndex,
                'filesSize': self.filesSize,
            }
            peer_to_server_socket.sendall(json.dumps(cmd_issue))
            rcv_data = json.loads(peer_to_server_socket.recv(1024))
            peer_to_server_socket.close()
            if rcv_data[1]:
                self.hosting_port = availablePort
                self.peer_id = rcv_data[0] + ":" + str(availablePort)
                self.prettyPeerID = rcv_data[2]

            else:
                print "Something went wrong when registering peer, Peer ID: %s:%s" \
                      % (rcv_data[0], availablePort)
                sys.exit(1)
        except Exception as e:
            print "Registering Peer Error, %s" % e
            sys.exit(1)

    """
    Obtain names of the all the files tracker has access to
    we will return all the names to the peer
    """
    def obtainAllSharedFiles(self):
        try:
            peerTrSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerTrSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            peerTrSocket.connect((sys.argv[1], self.server_port))

            cmd_issue = {
                'command': 'list'
            }
            peerTrSocket.sendall(json.dumps(cmd_issue))
            rcv_data = json.loads(peerTrSocket.recv(1024))
            peerTrSocket.close()
            myFiles = list()

            '''We will loop for all files in the tracker index'''
            newFile = False
            for f in rcv_data:
                '''For those files that we don't have'''
                if f not in self.fileIndex:
                    newFile = True
                    address = self.peerAddress(f)
                    self.obtain(f, address)

            return newFile


        except Exception as e:
            print "Listing Files from Index Server Error, %s" % e

    '''
    Obtain any of the peer address that have the file
    '''

    def peerAddress(self, filename):
        try:
            peerTrSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerTrSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            peerTrSocket.connect((sys.argv[1], self.server_port))

            cmd_issue = {
                'command': 'search',
                'file_name': filename
            }
            peerTrSocket.sendall(json.dumps(cmd_issue))
            rcv_data = json.loads(peerTrSocket.recv(1024))
            '''
            if len(rcv_data) == 0:
                print "File Not Found"


            print "\nFile Present in below following Peers:"
            '''

            for peer in rcv_data:
                if peer == self.peer_id:
                    print "File Present Locally, Peer ID: %s" % peer

            peerTrSocket.close()
            return peer
        except Exception as e:
            print "Search File Error, %s" % e

    '''
    This method will be called from obtainAll for each file present on the index of the tracker that we don't have
    and we will obtain the file from the corresponding peer
    '''

    def obtain(self, file_name, peer_request_id):

        try:
            peer_request_addr, peer_request_port = peer_request_id.split(':')
            peer_request_socket = \
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_request_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            peer_request_socket.connect(
                (peer_request_addr, int(peer_request_port)))

            peerTrSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peerTrSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            peerTrSocket.connect((sys.argv[1], self.server_port))

            cmd_issue = {
                'command': 'obtain_active',
                'file_name': file_name
            }

            peer_request_socket.sendall(json.dumps(cmd_issue))
            rcv_data = peer_request_socket.recv(512)
            f = open(SHAREDDIR + '/' + file_name, 'wb')
            counter = 0  # type: int

            while (rcv_data):
                f.write(rcv_data)
                rcv_data = peer_request_socket.recv(512)
                counter += 1
            cmd_issue = {
                'command': 'chunkUpdate',
                'peerID': self.peer_id,
                'prettyID': self.prettyPeerID,
                'fileName': file_name,
                'chunk': counter
            }
            #We send the number of chucks received
            peerTrSocket.sendall(json.dumps(cmd_issue))
            f.close()
            self.fileIndex.append(file_name)
            peerTrSocket.close()
            peer_request_socket.close()
        except Exception as e:
            print "Obtain File Error, %s" % e

    def deregisterPeer(self):
        """
        Deregister peer from Central Index Server.
        """
        try:

            peer_to_server_socket = \
                socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            peer_to_server_socket.setsockopt(
                socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            peer_to_server_socket.connect(
                (sys.argv[1], self.server_port))

            cmd_issue = {
                'command': 'deregister',
                'peer_id': self.peer_id,
                'prettyID': self.prettyPeerID,
                'files': self.fileIndex,
                'hosting_port': self.hosting_port
            }
            peer_to_server_socket.sendall(json.dumps(cmd_issue))
            rcv_data = json.loads(peer_to_server_socket.recv(1024))
            peer_to_server_socket.close()
            if rcv_data:
                print "PEER %s SHUTDOWN: HAS %s" % (self.prettyPeerID,
                                                    len(self.fileIndex))
                for f in self.fileIndex:
                    print "%s    %s" % (self.prettyPeerID, f)
            else:
                print "Error" \
                      % (self.peer_id)
        except Exception as e:
            print "Deregistering Peer Error, %s" % e


if __name__ == '__main__':
    """
    Main method starting deamon threads and peer operations.
    """
    try:
        # In finalTime we will store the time in which we want to derister the peer
        finalTime = datetime.datetime.now() + datetime.timedelta(0, float(sys.argv[3]))

        p = Peer()
        p.regisPeerTracker()

        '''
        "Stating peer tracker thread"
        '''
        trackerThread = PeerOperations(1, "PeerServer", p, finalTime)
        trackerThread.setDaemon(True)
        trackerThread.start()

        '''
        "Starting file handler thread (used by other peers)"
        '''
        file_handler_thread = PeerOperations(2, "PeerFileHandler", p, finalTime)
        file_handler_thread.setDaemon(True)
        file_handler_thread.start()

        while datetime.datetime.now()<finalTime:
            try:
                p.obtainAllSharedFiles()

            except Exception as e:
                print "Error obtaining files"
            time.sleep(15)
        '''
        Arriving here will mean the minimum time will have passed, we will keep looping however until there are no
        more files left to be downloaded
        '''
        filesLeft = True
        while filesLeft:
            try:
                filesLeft = p.obtainAllSharedFiles()

            except Exception as e:
                print "Error obtaining files"

        '''
        We will lastly disconnect the peer
        '''
        try:
            p.deregisterPeer()
        except Exception as e:
            print "Error deregistering peer"

    except Exception as e:
        print "Error", e
        sys.exit(1)

    except (SystemExit):
        p.deregisterPeer()
        time.sleep(1)
        sys.exit(1)
