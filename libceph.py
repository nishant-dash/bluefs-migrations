import subprocess
import json
import re

def lsblk():
    cmd = '/usr/bin/lsblk -a -J'
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
    return json.loads(output)

def ceph_volume_lvm_list():
    cmd = '/usr/sbin/ceph-volume lvm list --format json'
    output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
    return json.loads(output)

def get_disks():
    blockdevices = lsblk()['blockdevices']
    blockdevices_count = len(blockdevices)
    counter = 0
    while counter < blockdevices_count:
        blockdevice = blockdevices[counter]
        if blockdevice['type'] == 'loop':
            blockdevices.remove(blockdevice)
            blockdevices_count = blockdevices_count - 1
        else:
            counter = counter + 1
    return blockdevices

def is_osd(blockdevice_dict):
### check if this block device is an osd device
### only one path, no multiple leaf
    if 'children' not in blockdevice_dict.keys():
        if blockdevice_dict['name'].startswith('ceph'):
            return True
        else:
            return False
    elif len(blockdevice_dict['children']) == 1:
        return is_osd(blockdevice_dict['children'][0])
    else:
        return False

def get_path(blockdevice_dict, parent_devices=[]):
    if 'children' not in blockdevice_dict.keys():
        parent_devices.append(blockdevice_dict['name'])
        return '/'.join(parent_devices)
    else:
        parent_devices.append(blockdevice_dict['name'])
        return get_path(blockdevice_dict['children'][0], parent_devices)

def is_bcache_osd_device(blockdevice_dict):
    is_bcache_osd_device = False
    osd_count = 0
    for item in blockdevice_dict['children']:
        if is_osd(item):
            osd_count += 1
    if osd_count > 1:
        is_bcache_osd_device = True
    return is_bcache_osd_device

def get_osds_on_bcache(blockdevice_dict):
    osds_path = []
    for item in blockdevice_dict['children']:
        get_path(item, osd_path)
    return osds_path

def get_ceph_fsid(blockdev_path):
    rslt = re.search(r'(?<=ceph)(--\w*)*', blockdev_path)
    raw_fsid = rslt.group(0)
    fsid = raw_fsid[2:].replace('--','-')
    return fsid

def get_osds():
    osds = dict()
    ceph_volumes = ceph_volume_lvm_list()
    for osd in ceph_volumes.keys():
        osds[osd] = dict()
        for specs in ceph_volumes[osd]:
            if specs['type'] == 'block':
                osds[osd]['device'] = specs['devices']
                osds[osd]['fsid'] = specs['tags']['ceph.osd_fsid']
            elif specs['type'] == 'db':
                osds[osd]['db'] = specs['devices']
    return osds


def get_osds():
    osds = dict()
    ceph_volumes = ceph_volume_lvm_list()
    for osd in ceph_volumes.keys():
        osds[osd] = dict()
        for specs in ceph_volumes[osd]:
            if specs['type'] == 'block':
                osds[osd]['device'] = specs['devices']
                osds[osd]['fsid'] = specs['tags']['ceph.osd_fsid']
            elif specs['type'] == 'db':
                osds[osd]['db'] = specs['devices']
    return osds

def get_osds_map():
    disks = get_disks()
    osds = get_osds()
    for disk in disks:
        path = []
        if is_osd(disk):
            path = get_path(disk, path)
            fsid = get_ceph_fsid(path)
            osd_filter = filter(lambda x: True if x[1]['fsid'] == fsid else False, osds.items())
            osd = list(osd_filter)
            print((osd, path.split('/')[0]))
