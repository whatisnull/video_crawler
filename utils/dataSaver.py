#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys, os, time, getopt, re, commands

class DataSaver(object):
	def __init__(self, params=None):
		self._roll_policy	= None
		self.wfp = None
		self.dfile = None
		if params is not None:
			self.set_filename_format(params)

	def __del__(self):
#		print "destruction called"
		if self.wfp is not None:
			self.close_file()

	def set_filename_format(self, params):
		''' roll_policy: 
				time:	minute, hour, day, week, month
				size:  KB, MB, GB
			namefmt:	format for filename, can include %t %[n]d other than normal chars
			timefmt:	time format if namefmt include %t
		'''
		if params is None or not params.has_key('roll_policy') or not params.has_key('namefmt'):
			print "parameters is not valid:"
			print params
			return
		rps = params['roll_policy'].split(":")
		if rps[0].lower() == "time":
			ts = rps[1].lower()
			if ts == "minute": self._rolltfmt = "%Y%m%d%H%M"
			elif ts == "hour": self._rolltfmt = "%Y%m%d%H"
			elif ts == "week": self._rolltfmt = "%Y%m%d%H"
			elif ts == "month": self._rolltfmt = "%Y%m"
			else: self._rolltfmt = "%Y%m%d"
			self._roll_policy = 1
#			print "rollfmt limit is: ", self._rolltfmt
		else:
			ts = rps[1].lower().strip()
			self._rollsize = int(ts[0:-2])
			ts = ts[-2:]
			if ts == "gb": ml = 1024*1024*1024
			elif ts == "kb": ml = 1024
			else: ml = 1024 * 1024
			self._rollsize = self._rollsize * ml
			self._roll_policy = 2
#			print "size limit is: ", self._rollsize
		self._filename_pat=[]
		iofs = ic = ix = 0
		while True:
			idx = params['namefmt'].find('%', iofs)
			if idx > 0:
				self._filename_pat.append((0, params['namefmt'][iofs:idx]))	# normal string
			elif idx < 0:
				self._filename_pat.append((0, params['namefmt'][iofs:]))		# normal string
				break
			if idx + 2 > len(params['namefmt']): break
			ps = params['namefmt'][idx:idx+2].lower()
			if ps == "%t":
				if params.has_key('timefmt') and ix < len(params['timefmt']):
					self._filename_pat.append((1, params['timefmt'][ix]))		# time string
					ix = ix + 1
				iofs = idx + 2
			elif ps == "%d":
				self._filename_pat.append((2, ps))								# file index
				iofs = idx + 2
				self._file_idx = 0
			else:
				if idx + 3 > len(params['namefmt']): break
				ps = params['namefmt'][idx:idx+3].lower()
				if ps[2:3] == 'd' and ps[1:2] >= '0'  and ps[1:2] <= '9':
					self._filename_pat.append((2, '%0'+ps[1:]))							# file index with fixed width
					iofs = idx + 3
					self._file_idx = 0
				else: iofs = idx + 1
#		print self._filename_pat

	def check_dir(self, fname):
		idx = fname.rfind("/")
		if idx < 0: return True
		fdir = fname[0:idx]
		if len(fdir) < 1: return True
		if os.path.exists(fdir): return True
		try:
			(status, output) = commands.getstatusoutput("mkdir -p '%s'"%fdir)
			return status == 0
		except Exception, e:
			print "cannot create dir: %s, %s"%(fdir, e)
		return False

	def get_filename(self):
		if self.dfile is not None and not self.file_need_roll():
			return self.dfile
		fname=""
		for fld in self._filename_pat:
			if fld[0] == 0:
				fname = fname + fld[1]
			elif fld[0] == 1:
				fname = fname + time.strftime(fld[1])
			elif fld[0] == 2:
				fname = fname + fld[1]%self._file_idx
		return fname

	def file_need_roll(self):
		if self._roll_policy == 1:
			time_tag = time.strftime(self._rolltfmt)
			if len(self._file_tag) > 0 and time_tag != self._file_tag: return True
		else:
#			print "file size: %d %d"%(self._file_size, self._rollsize)
			if self._file_size >= self._rollsize: return True
		return False

	def create_file(self):
		if self.wfp is not None:
			self.close_file()
		self.dfile	= self.get_filename()
		if self._roll_policy == 1:
			self._file_tag  = time.strftime(self._rolltfmt)
		else:
			self._file_size = 0
			self._file_idx = self._file_idx + 1
#			print "fileidx = %d"%self._file_idx
		if self.check_dir(self.dfile):
			self.wfp = open(self.dfile, "a+")

	def save_data(self, data, head_data=None, no_head=False):
		if self.wfp is None or (self._roll_policy == 1 and self.file_need_roll()):
			self.create_file()
		if self.wfp is None:
			raise Exception("dataSaver: Create file failed: %s"%self.dfile)

		if not no_head:
			if head_data is None:
				head_data={}
			for key in head_data:
				if key != "length" and key != "write_time":
					self.wfp.write(key + ":" + head_data[key] + "\n")
			self.wfp.write("write_time:%s\n"%time.strftime("%Y-%m-%d %H:%M:%S"))
			self.wfp.write("length:%d\n"%len(data))
		self.wfp.write(data)
		self._file_size = self.wfp.tell()
		if self._roll_policy == 2 and self.file_need_roll():
			self.close_file()

	def close_file(self):
		if self.wfp is None: return
		try:
			flen = self.wfp.tell()
			self.wfp.close()
			if flen < 1:						# remove file while it's length is zero
				os.remove(self.dfile)
		except Exception, e:
			print "close file %s failed, %s"%(self.dfile, e)
		self.wfp = None

#if __name__ == '__main__':
#	ods = DataSaver()
#	policy={'roll_policy':'time:minute', 'namefmt':'data%/%t/%t/res_%t.dat', 'timefmt':['%Y','%Y%m%d','%Y%m%d_%H%M%S']}
#	policy={'roll_policy':'size:1kb', 'namefmt':'data/%t/%t/test-%3d.dat', 'timefmt':['%Y','%Y%m%d','%Y%m%d_%H%M%S']}
#	ods.set_filename_format(policy)
#	for i in range(0, 10):
#		print "%2d ==> %s"%(i, ods.get_filename())
##		ods.save_data("I'm No.%d\n"%i, no_head=True)
#		ods.save_data("write more data\naaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
#		ods.save_data("write more data\nbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
#		ods.save_data("write more data\ncccbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
#		ods.save_data("write more data\ndddbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
#		ods.save_data("write more data\neeebbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
#		time.sleep(1)

