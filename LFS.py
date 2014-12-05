#!/usr/bin/python
import sys, struct
import Segment
import InodeMap

from threading import Thread, Lock, Condition, Semaphore
from Segment import SegmentManagerClass
from Disk import DiskClass
from Inode import Inode, getmaxinode, setmaxinode
from InodeMap import InodeMapClass
from FileDescriptor import FileDescriptor
from DirectoryDescriptor import DirectoryDescriptor
from Constants import FILENAMELEN
from FSE import FileSystemException
import Disk

def find_parent_name(path):
    parent, sep, element = path.rpartition("/")
    if parent == '':
        parent = '/'
    return parent

def find_filename(path):
    parent, sep, element = path.rpartition("/")
    return element

#takes an absolute path, iterates through the components in the name
def get_path_components(path):
    for component in path[1:].strip().split("/"):
        yield component

class LFSClass:
    def __init__(self, initdisk=True):
        pass

    # open an existing file or directory
    def open(self, path, isdir=False):
        inodenumber = self.searchfiledir(path)
        if inodenumber is None:
            raise FileSystemException("Path Does Not Exist")
        # create and return a Descriptor of the right kind
        if isdir:
            return DirectoryDescriptor(inodenumber)
        else:
            return FileDescriptor(inodenumber)

    def create(self, filename, isdir=False):
        fileinodenumber = self.searchfiledir(filename)
        if fileinodenumber is not None:
            raise FileSystemException("File Already Exists")

        # create an Inode for the file
        # Inode constructor writes the inode to disk and implicitly updates the inode map
        newinode = Inode(isdirectory=isdir)

        # now append the <filename, inode> entry to the parent directory
        parentdirname = find_parent_name(filename)
        parentdirinodenumber = self.searchfiledir(parentdirname)
        if parentdirinodenumber is None:
            raise FileSystemException("Parent Directory Does Not Exist")
        parentdirblockloc = InodeMap.inodemap.lookup(parentdirinodenumber)
        parentdirinode = Inode(str=Segment.segmentmanager.blockread(parentdirblockloc))
        self.append_directory_entry(parentdirinode, find_filename(filename), newinode)

        if isdir:
            return DirectoryDescriptor(newinode.id)
        else:
            return FileDescriptor(newinode.id)

    # return metadata about the given file
    def stat(self, pathname):
        inodenumber = self.searchfiledir(pathname)
        if inodenumber is None:
            raise FileSystemException("File or Directory Does Not Exist")

        inodeblocknumber = InodeMap.inodemap.lookup(inodenumber)
        inodeobject = Inode(str=Segment.segmentmanager.blockread(inodeblocknumber))
        return inodeobject.filesize, inodeobject.isDirectory

    def rmdir(self, pathname):
	(sz, isdir) = self.stat(pathname)
	if (not isdir):
		raise FileSystemException(pathname + " is not a directory. Remove it with rm.")
        inodenumber = self.searchfiledir(pathname)
	descriptor = DirectoryDescriptor(inodenumber)
	entries = descriptor.enumerate()
	i = 0
	for (k,v) in entries:
		i += 1
	if (i > 0):
		raise FileSystemException(pathname + " is not empty. Can only delete empty directories.")
        parentdirname = find_parent_name(pathname)
        parentdirinodenumber = self.searchfiledir(parentdirname)
        if parentdirinodenumber is None:
            raise FileSystemException("Parent Directory Does Not Exist")
        self.remove_directory_entry(parentdirinodenumber, find_filename(pathname))

    # delete the given file
    def unlink(self, pathname):
	(sz,isdir) = self.stat(pathname)
	if (isdir):
		raise FileSystemException(pathname + " is a directory. Remove it with rmdir.")
	fileinodenumber = self.searchfiledir(pathname)
        if fileinodenumber is None:
            print "File does not exist " + pathname
	    return


        # now append the <filename, inode> entry to the parent directory
        parentdirname = find_parent_name(pathname)
        parentdirinodenumber = self.searchfiledir(parentdirname)
        if parentdirinodenumber is None:
            raise FileSystemException("Parent Directory Does Not Exist")
        self.remove_directory_entry(parentdirinodenumber, find_filename(pathname))

        pass

    # write all in memory data structures to disk
    def sync(self):
	(data, num) = InodeMap.inodemap.save_inode_map(getmaxinode())
	# replace this with segmentmanager.write_to_newblock after implementing that method
	location = Segment.segmentmanager.currentseg.write_to_newblock(data)
	if (location < 0):
		print "failed to write inodemap to disk"
		os._exit(1)
	Segment.segmentmanager.update_inodemap_position(location, num)
	Segment.segmentmanager.flush()
	
        pass

    # restore in memory data structures (e.g. inode map) from disk
    def restore(self):
        imlocation = Segment.segmentmanager.locate_latest_inodemap()
        iminode = Inode(str=Disk.disk.blockread(imlocation))
        imdata = iminode.read(0, 10000000)
        # restore the latest inodemap from wherever it may be on disk
        setmaxinode(InodeMap.inodemap.restore_inode_map(imdata))

    # for a given file or directory named by path,
    # return its inode number if the file or directory exists,
    # else return None
    def searchfiledir(self, path):
        # XXX - do this tomorrow! after the meteor shower!
	nextid = 1
	nextpathentry = 0
	pathentries = path[1:].split("/")
	print pathentries
	if (pathentries[0] == ""):
		return nextid
	while (nextpathentry < len(pathentries)):
		curfd = DirectoryDescriptor(nextid)
		entries = curfd.enumerate()
		found = 0
		for (name, inode) in entries:
			if (name == pathentries[nextpathentry]):
				    nextid = inode
				    found = 1
				    nextpathentry = nextpathentry + 1
				    break
		if (found == 0):
			print "file " + path + " not found"
			return None
	return nextid
        pass

    # add the new directory entry to the data blocks,
    # write the modified inode to the disk,
    # and update the inode map
    def append_directory_entry(self, dirinode, filename, newinode):
        dirinode.write(dirinode.filesize, struct.pack("%dsI" % FILENAMELEN, filename, newinode.id))

    def remove_directory_entry(self, dirinodenum, filename):
	dir = DirectoryDescriptor(dirinodenum)
	dir.unlink(filename)	

filesystem = None
