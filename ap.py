from pathlib import Path
import subprocess as sp
import json
import dataclasses
import argparse
import uuid
# try to stick to std libs only


@dataclasses.dataclass(kw_only=True)
class CephOsdMigrationData:
	device_type: str = ""
	osd_fsid: str = ""
	osd_id: str = ""
	data_lv: str = ""
	data_vg: str = ""
	db_lv: str = ""
	fast_ceph: bool = False

	def migrate_to_dev(self) -> str:
		"""Here the target is 'db wal' because its a dedicated device for bluefs db, the lvm migrate cmd will only migrate bluefs db data."""
		return f"ceph-volume lvm migrate --osd-id {self.osd_id} --osd-fsid {self.osd_fsid} --from db wal --target {self.data_vg}/{self.data_lv}"

	def migrate_to_target_lv(self, target) -> str:
		"""Here the source is 'data' because its a shared device for data and bluefs db, the lvm migrate cmd will only migrate bluefs db data. """
		return f"ceph-volume lvm migrate --osd-id {self.osd_id} --osd-fsid {self.osd_fsid} --from data --target {target}"

	def create_new_db(self, target) -> str:
		return f"ceph-volume lvm new-db --osd-id {self.osd_id} --osd-fsid {self.osd_fsid} --target {target}"



def print_divide() -> None:
	if not ONLY_SHOW_MIGRATION_CONTEXT:
		print("-"*80)


def gprint(text) -> None:
	print(f"\033[92m{text}\033[0m")


def pprint(text: str) -> None:
	if not ONLY_SHOW_MIGRATION_CONTEXT:
		print(text)


def gpath(disk) -> str:
	# return f"/dev/disk/by-dname/{disk}" # pvcreate cant find the disk this way
	return f"/dev/{disk}"


def extract_dev_name_from_md_stat_output_final(dev_name_with_raid_num):
	return dev_name_with_raid_num.split("[")[0]


def get_devs_proc_md_stat(raid_name) -> list[str]:
	# assumes raid0
	output = sp.run(["cat", "/proc/mdstat"], stdout=sp.PIPE, stderr=sp.DEVNULL, text=True)
	for l in output.stdout.split("\n"):
		if l.startswith(raid_name):
			line_elems = l.split(" ")
			d0 = line_elems[-2]
			d1 = line_elems[-1]
			return [extract_dev_name_from_md_stat_output_final(d0), extract_dev_name_from_md_stat_output_final(d1)]

	return ["", ""]


def find_candidate_nvme_for_slow_dbs() -> str:
	if SLOW_CEPH_DB_DEVICE:
		print(f"Using {SLOW_CEPH_DB_DEVICE}")
		return SLOW_CEPH_DB_DEVICE

	cmd = ["lsblk", "--json"]
	output = sp.run(cmd, stdout=sp.PIPE, stderr=sp.DEVNULL)
	lsblk = json.loads(output.stdout)

	candidates = []
	candidates_info = []

	for dev in lsblk["blockdevices"]:
		if dev["name"].startswith("nvme"):
			for child_dev in dev.get("children", {}):
				if child_dev["name"].startswith("nvme"):
					if ("children" not in child_dev) and (not child_dev["mountpoints"][0]):
						candidates_info.append(child_dev)
						candidates.append(child_dev["name"])

	if len(candidates) != 1:
		for c in candidates_info:
			print(json.dumps(c, indent=4))
		raise RuntimeError(f"Found none or more than one candidate nvme for slow ceph dbs, got {candidates}, I don't know what to do...\n If you know which one to choose, run the script again but specify '-d DEVICE_NAME'")
	else:
		return candidates[0]


def ceph_lv_info(osd_info: dict) -> (CephOsdMigrationData, set):
	osd_lv_tags = osd_info['tags']
	ceph_db_device_in_tag = osd_lv_tags["ceph.db_device"]
	is_block_device = osd_info["type"].lower() == "block"
	migration_data = CephOsdMigrationData()
	migration_data.device_type = osd_info["type"].lower()
	zap_data = set()

	pprint(f"This is a {osd_info['type']} device")
	pprint(f"Devices are {osd_info['devices']}")
	pprint(f"  LV path {osd_info['lv_path']}")
	pprint(f"  LV size is {osd_info['lv_size']} bytes or {int(osd_info['lv_size']) / (1024 ** 3)} GiB")
	pprint(f"  LV {osd_info['lv_name']}")

	if is_block_device:
		pprint(f"  The DB device according to LV tags {ceph_db_device_in_tag}")

	if is_block_device:
		migration_data.osd_fsid = osd_lv_tags["ceph.osd_fsid"]
		migration_data.osd_id = osd_lv_tags["ceph.osd_id"]
		migration_data.data_lv = osd_info["lv_name"]
		migration_data.data_vg = osd_info["vg_name"]
		full_db_path = osd_lv_tags["ceph.db_device"]
		migration_data.db_lv = full_db_path.split('/')[-1]

		if len(osd_info['devices']) > 1:
			raise RuntimeError(f"I cant process osd.{osd_lv_tags['ceph.osd_id']}, it has more than one device {osd_info['devices']}")

		osd_dev = osd_info['devices'][0]
		migration_data.fast_ceph = "nvme" in osd_dev

	if not is_block_device:
		for d in osd_info['devices']:
			zap_data.add(d)

	if migration_data.fast_ceph:
		pprint("This is fast ceph")

	pprint("")

	return migration_data, zap_data


def ceph_lvm_cmd(osd_id: int) -> None:
	cmd = ["ceph-volume", "lvm", "list", osd_id, "--format", "json"]
	output = sp.run(cmd, stdout=sp.PIPE, stderr=sp.DEVNULL)
	output = json.loads(output.stdout)
	return output


def generate_ap(all_osds: list[int]) -> None:
	zap_parts = set()
	migrations_data = []

	for osd_id in all_osds:
		pprint(f"Checking OSD {osd_id}")
		print_divide()

		ceph_lvm_output = ceph_lvm_cmd(osd_id)
		if not ceph_lvm_output:
			return

		for osd_info in ceph_lvm_output[osd_id]:
			migration_data, zap_data = ceph_lv_info(osd_info)
			if migration_data:
				if migration_data.device_type == "block":
					migrations_data.append(migration_data)
			if zap_data:
				for d in zap_data:
					zap_parts.add(d)

		print_divide()
		pprint("")


	osds_string = ','.join(all_osds)

	free_nvme = gpath(find_candidate_nvme_for_slow_dbs())
	fast_ceph_db_vgs = [f"ceph-db-{uuid.uuid4()}" for _ in range(2)]
	fast_ceph_db_lvs = [f"osd-db-{uuid.uuid4()}" for _ in range(2)]

	slow_ceph_db_vg = "slow-ceph-db-vg"
	slow_ceph_db_lvs = [f"osd-db-{uuid.uuid4()}" for _ in range(4)]

	if len(zap_parts) != 1:
		print(f"Something went wrong... I should only need to tell you to zap 1 raid, but I got {zap_parts}")
		return

	raid_dev_name = list(zap_parts)[0]
	raid_name = raid_dev_name.split("/")[-1]

	raid_devs_names = get_devs_proc_md_stat(raid_name)
	if not raid_devs_names[0] and raid_devs_names[1]:
		print("Could not get raid0 devices from 'cat /proc/mdstat'")
		return

	raid_devs = [gpath(d) for d in raid_devs_names]

	gprint("# Step 1: Check ceph health and set ceph noout and stop osds")
	print(f"sudo systemctl stop ceph-osd@{{{osds_string}}}")

	print()
	gprint("# Step 2: Command for migration")
	for osd_migration in migrations_data:
		print(osd_migration.migrate_to_dev())
	pprint("")

	print()
	gprint("# Step 3: Command for Zap")
	print(f"ceph-volume lvm zap --destroy /dev/disk/by-dname/{raid_name}")
	pprint("")

	print()
	gprint("# Step 4: Delete RAID0 setup for bluefs DB storage")
	print(f"mdadm --detail /dev/disk/by-dname/{raid_name}")
	print(f"mdadm --stop /dev/disk/by-dname/{raid_name}")
	print(f"mdadm --detail /dev/disk/by-dname/{raid_name}")
	pprint("")
	print(f"mdadm --examine {raid_devs[0]}")
	print(f"mdadm --zero-superblock {raid_devs[0]}")
	print(f"mdadm --examine {raid_devs[0]}")

	print(f"mdadm --examine {raid_devs[1]}")
	print(f"mdadm --zero-superblock {raid_devs[1]}")
	print(f"mdadm --examine {raid_devs[1]}")
	pprint("")

	gprint(f"# Step 5: Create PVs and VGs on the candidate nvme for slow and nvmes freed up from raid {raid_name} for fast")

	print(f"pvcreate {free_nvme}")
	print(f"vgcreate {slow_ceph_db_vg} {free_nvme}")

	print(f"pvcreate {raid_devs[0]}")
	print(f"pvcreate {raid_devs[1]}")
	print(f"vgcreate {fast_ceph_db_vgs[0]} {raid_devs[0]}")
	print(f"vgcreate {fast_ceph_db_vgs[1]} {raid_devs[1]}")
	pprint("")


	print()
	gprint("# Step 6.1: Create 4x slow DB")
	for slow_ceph_db_lv in slow_ceph_db_lvs:
		print(f"lvcreate -n {slow_ceph_db_lv} {slow_ceph_db_vg} -l 25%VG")

	print()
	gprint("# Step 6.2: Create 2x fast DB")
	print(f"lvcreate -n {fast_ceph_db_lvs[0]} {fast_ceph_db_vgs[0]} -l 100%VG")
	print(f"lvcreate -n {fast_ceph_db_lvs[1]} {fast_ceph_db_vgs[1]} -l 100%VG")

	print()
	gprint("# Step 7.1: Create new db and migrate, for slow ceph")
	ctr = 0
	for osd_info in migrations_data:
		if not osd_info.fast_ceph:
			target = f"{slow_ceph_db_vg}/{slow_ceph_db_lvs[ctr]}"
			print(osd_info.create_new_db(target))
			print(osd_info.migrate_to_target_lv(target))
			ctr += 1

	print()
	gprint("# Step 7.2: Create new db and migrate, for fast ceph")
	ctr = 0
	for osd_info in migrations_data:
		mod_idx = ctr % 2
		if osd_info.fast_ceph:
			target = f"{fast_ceph_db_vgs[mod_idx]}/{fast_ceph_db_lvs[mod_idx]}"
			print(osd_info.create_new_db(target))
			print(osd_info.migrate_to_target_lv(target))
			ctr += 1

	print()
	gprint("# Step 8: Review block assignments and check everything")
	print("lsblk")
	print("ceph-volume lvm list")

	print()
	gprint("# Step 9: Start up the osds")
	print(f"sudo systemctl start ceph-osd@{{{osds_string}}}")
	print(f"sudo systemctl is-active ceph-osd@{{{osds_string}}}")

	print()
	gprint("# Step 10: Monitor ceph status and health. Worst case, if the data is messed up, mark the osds out and let the data get recreated.")



def main() -> None:
	all_osds = []

	for d in Path("/var/lib/ceph/osd").glob("ceph-*"):
	    whoami_file = d / "whoami"
	    if whoami_file.is_file():
	        with whoami_file.open() as f:
	            osd_id = f.read().strip()
	            all_osds.append(osd_id)

	pprint(f"OSDs are: {all_osds}")
	print_divide()

	generate_ap(all_osds)


if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("-a", "--all", help="Print detailed information", action="store_true")
	parser.add_argument("-d", "--device", help="Device to use to create the VG for the LVs to store the slo ceph DBs")
	args = parser.parse_args()

	global ONLY_SHOW_MIGRATION_CONTEXT
	ONLY_SHOW_MIGRATION_CONTEXT = True

	global SLOW_CEPH_DB_DEVICE
	SLOW_CEPH_DB_DEVICE = args.device

	if args.all:
		ONLY_SHOW_MIGRATION_CONTEXT = not args.all

	main()

