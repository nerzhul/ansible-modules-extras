#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# This module is also sponsored by E.T.A.I. (www.etai.fr)
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

ANSIBLE_METADATA = {'status': ['preview'],
                    'supported_by': 'community',
                    'version': '1.0'}

DOCUMENTATION = '''
---
module: vmware_guest
short_description: Manages virtualmachines in vcenter
description:
    - Uses pyvmomi to ...
    - copy a template to a new virtualmachine
    - poweron/poweroff/restart a virtualmachine
    - remove a virtualmachine
version_added: 2.2
author:
    - James Tanner (@jctanner) <tanner.jc@gmail.com>
    - Loic Blot (@nerzhul) <loic.blot@unix-experience.fr>
notes:
    - Tested on vSphere 6.0
requirements:
    - "python >= 2.6"
    - PyVmomi
options:
   state:
        description:
            - What state should the virtualmachine be in?
        required: True
        choices: ['present', 'absent', 'poweredon', 'poweredoff', 'restarted', 'suspended']
   name:
        description:
            - Name of the newly deployed guest
        required: True
   name_match:
        description:
            - If multiple vms matching the name, use the first or last found
        required: False
        default: 'first'
        choices: ['first', 'last']
   uuid:
        description:
            - UUID of the instance to manage if known, this is vmware's unique identifier.
            - This is required if name is not supplied.
        required: False
   template:
        description:
            - Template used to create guest.
            - If this value is not set, VM is created without using a template.
            - If the guest exists already this setting will be ignored.
        required: False
   template_flag:
        description:
            - Flag the instance as a template
        required: False
        default: False
        version_added: "2.3"
   folder:
        description:
            - Destination folder path for the new guest
        required: False
   hardware:
        description:
            - Attributes such as cpus, memory, osid, and disk controller
        required: False
   disk:
        description:
            - A list of disks to add
        required: False
   wait_for_ip_address:
        description:
            - Wait until vcenter detects an IP address for the guest
        required: False
   force:
        description:
            - Ignore warnings and complete the actions
        required: False
   datacenter:
        description:
            - Destination datacenter for the deploy operation
        required: True
   cluster:
        description:
            - The cluster name where the VM will run.
        required: False
        version_added: "2.3"
   esxi_hostname:
        description:
            - The esxi hostname where the VM will run.
        required: False
   annotation:
        description:
            - A note or annotation to include in the VM
        required: False
        version_added: "2.3"
   customize:
        description:
           - Should customization spec be applied. This is only used when deploying a template.
        required: False
        version_added: "2.3"
   networks:
        description:
          - Network to use should include VM network name and gateway
        required: False
        version_added: "2.3"
   dns_servers:
        description:
          - DNS servers to use
        required: False
        version_added: "2.3"
   domain:
        description:
          - Domain to use while customizing
        required: False
        version_added: "2.3"
   snapshot_op:
        description:
          - A key, value pair of snapshot operation types and their additional required parameters.
        required: False
        version_added: "2.3"
extends_documentation_fragment: vmware.documentation
'''

EXAMPLES = '''
Example from Ansible playbook
#
# Create a VM from a template
#
    - name: create the VM
      vmware_guest:
        validate_certs: False
        hostname: 192.0.2.44
        username: administrator@vsphere.local
        password: vmware
        name: testvm_2
        state: poweredon
        folder: testvms
        disk:
            - size_gb: 10
              type: thin
              datastore: g73_datastore
        hardware:
            memory_mb: 512
            num_cpus: 1
            osid: centos64guest
            scsi: paravirtual
        datacenter: datacenter1
        esxi_hostname: 192.0.2.117
        template: template_el7
        wait_for_ip_address: yes
      register: deploy

#
# Clone Template and customize
#
   - name: Clone template and customize
     vmware_guest:
       hostname: "192.168.1.209"
       username: "administrator@vsphere.local"
       password: "vmware"
       validate_certs: False
       name: testvm-2
       datacenter: datacenter1
       cluster: cluster
       validate_certs: False
       template: template_el7
       customize: True
       domain: "example.com"
       dns_servers: ['192.168.1.1','192.168.1.2']
       networks:
         '192.168.1.0/24':
           network: 'VM Network'
           gateway: '192.168.1.1'
           ip: "192.168.1.100"
#
# Gather facts only
#
    - name: gather the VM facts
      vmware_guest:
        validate_certs: False
        hostname: 192.168.1.209
        username: administrator@vsphere.local
        password: vmware
        name: testvm_2
        esxi_hostname: 192.168.1.117
      register: facts

### Snapshot Operations
# Create snapshot
  - vmware_guest:
     hostname: 192.168.1.209
     username: administrator@vsphere.local
     password: vmware
     validate_certs: False
     name: dummy_vm
     snapshot_op:
         op_type: create
         name: snap1
         description: snap1_description

# Remove a snapshot
  - vmware_guest:
     hostname: 192.168.1.209
     username: administrator@vsphere.local
     password: vmware
     validate_certs: False
     name: dummy_vm
     snapshot_op:
         op_type: remove
         name: snap1

# Revert to a snapshot
  - vmware_guest:
     hostname: 192.168.1.209
     username: administrator@vsphere.local
     password: vmware
     validate_certs: False
     name: dummy_vm
     snapshot_op:
         op_type: revert
         name: snap1

# List all snapshots of a VM
  - vmware_guest:
     hostname: 192.168.1.209
     username: administrator@vsphere.local
     password: vmware
     validate_certs: False
     name: dummy_vm
     snapshot_op:
         op_type: list_all

# List current snapshot of a VM
  - vmware_guest:
     hostname: 192.168.1.209
     username: administrator@vsphere.local
     password: vmware
     validate_certs: False
     name: dummy_vm
     snapshot_op:
         op_type: list_current

# Remove all snapshots of a VM
  - vmware_guest:
     hostname: 192.168.1.209
     username: administrator@vsphere.local
     password: vmware
     validate_certs: False
     name: dummy_vm
     snapshot_op:
         op_type: remove_all
'''

RETURN = """
instance:
    descripton: metadata about the new virtualmachine
    returned: always
    type: dict
    sample: None
"""

try:
    import json
except ImportError:
    import simplejson as json

HAS_PYVMOMI = False
try:
    import pyVmomi
    from pyVmomi import vim

    HAS_PYVMOMI = True
except ImportError:
    pass

import os
import time
from netaddr import IPNetwork, IPAddress

from ansible.module_utils.urls import fetch_url


class PyVmomiDeviceHelper(object):
    """ This class is a helper to create easily VMWare Objects for PyVmomiHelper """

    def __init__(self, module):
        self.module = module

    @staticmethod
    def create_scsi_controller():
        scsi_ctl = vim.vm.device.VirtualDeviceSpec()
        scsi_ctl.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        scsi_ctl.device = vim.vm.device.VirtualLsiLogicController()
        scsi_ctl.device.deviceInfo = vim.Description()
        scsi_ctl.device.slotInfo = vim.vm.device.VirtualDevice.PciBusSlotInfo()
        scsi_ctl.device.slotInfo.pciSlotNumber = 16
        scsi_ctl.device.controllerKey = 100
        scsi_ctl.device.unitNumber = 3
        scsi_ctl.device.busNumber = 0
        scsi_ctl.device.hotAddRemove = True
        scsi_ctl.device.sharedBus = 'noSharing'
        scsi_ctl.device.scsiCtlrUnitNumber = 7

        return scsi_ctl

    @staticmethod
    def create_scsi_disk(scsi_ctl):
        diskspec = vim.vm.device.VirtualDeviceSpec()
        diskspec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
        diskspec.fileOperation = vim.vm.device.VirtualDeviceSpec.FileOperation.create
        diskspec.device = vim.vm.device.VirtualDisk()
        diskspec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
        diskspec.device.backing.diskMode = 'persistent'
        diskspec.device.controllerKey = scsi_ctl.device.key

        return diskspec

    def create_nic(self, device_type, device_label, device_summary):
        nic = vim.vm.device.VirtualDeviceSpec()
        if device_type == 'vmxnet3':
            nic.device = vim.vm.device.VirtualVmxnet3()
        elif device_type == 'e1000':
            nic.device = vim.vm.device.VirtualE1000()
        else:
            self.module.fail_json(msg="invalid device_type '%s' for network %s" %
                                      (device_type, device_summary))

        nic.device.wakeOnLanEnabled = True
        nic.device.addressType = 'assigned'
        nic.device.deviceInfo = vim.Description()
        nic.device.deviceInfo.label = device_label
        nic.device.deviceInfo.summary = device_summary
        nic.device.connectable = vim.vm.device.VirtualDevice.ConnectInfo()
        nic.device.connectable.startConnected = True
        nic.device.connectable.allowGuestControl = True
        nic.device.connectable.connected = True
        nic.device.connectable.allowGuestControl = True

        return nic


class PyVmomiHelper(object):
    def __init__(self, module):
        if not HAS_PYVMOMI:
            module.fail_json(msg='pyvmomi module required')

        self.module = module
        self.device_helper = PyVmomiDeviceHelper(self.module)
        self.params = module.params
        self.si = None
        self.content = connect_to_api(self.module)
        self.datacenter = None
        self.folders = None
        self.foldermap = None
        self.configspec = None
        self.customspec = None

    def is_template(self):
        return 'template' in self.params and self.params['template'] is not None

    def _build_folder_tree(self, folder):

        tree = {'virtualmachines': [],
                'subfolders': {},
                'vimobj': folder,
                'name': folder.name}

        children = None
        if hasattr(folder, 'childEntity'):
            children = folder.childEntity

        if children:
            for child in children:
                if child == folder or child in tree:
                    continue
                if isinstance(child, vim.Folder):
                    ctree = self._build_folder_tree(child)
                    tree['subfolders'][child] = dict.copy(ctree)
                elif isinstance(child, vim.VirtualMachine):
                    tree['virtualmachines'].append(child)
        else:
            if isinstance(folder, vim.VirtualMachine):
                return folder
        return tree

    def _build_folder_map(self, folder, vmap={}, inpath='/'):

        """ Build a searchable index for vms+uuids+folders """

        if isinstance(folder, tuple):
            folder = folder[1]

        if 'names' not in vmap:
            vmap['names'] = {}
        if 'uuids' not in vmap:
            vmap['uuids'] = {}
        if 'paths' not in vmap:
            vmap['paths'] = {}

        if inpath == '/':
            thispath = '/vm'
        else:
            thispath = os.path.join(inpath, folder['name'])

        if thispath not in vmap['paths']:
            vmap['paths'][thispath] = []

        # helpful for isolating folder objects later on
        if 'path_by_fvim' not in vmap:
            vmap['path_by_fvim'] = {}
        if 'fvim_by_path' not in vmap:
            vmap['fvim_by_path'] = {}
        # store object by path and store path by object
        vmap['fvim_by_path'][thispath] = folder['vimobj']
        vmap['path_by_fvim'][folder['vimobj']] = thispath

        # helpful for isolating vm objects later on
        if 'path_by_vvim' not in vmap:
            vmap['path_by_vvim'] = {}
        if 'vvim_by_path' not in vmap:
            vmap['vvim_by_path'] = {}
        if thispath not in vmap['vvim_by_path']:
            vmap['vvim_by_path'][thispath] = []

        for item in folder.items():
            k = item[0]
            v = item[1]

            if k == 'name':
                pass
            elif k == 'subfolders':
                for x in v.items():
                    vmap = self._build_folder_map(x, vmap=vmap, inpath=thispath)
            elif k == 'virtualmachines':
                for x in v:
                    if x.config.name not in vmap['names']:
                        vmap['names'][x.config.name] = []
                    vmap['names'][x.config.name].append(x.config.uuid)
                    vmap['uuids'][x.config.uuid] = x.config.name
                    vmap['paths'][thispath].append(x.config.uuid)

                    if x not in vmap['vvim_by_path'][thispath]:
                        vmap['vvim_by_path'][thispath].append(x)
                    if x not in vmap['path_by_vvim']:
                        vmap['path_by_vvim'][x] = thispath
        return vmap

    def getfolders(self):

        if not self.datacenter:
            self.get_datacenter()
        self.folders = self._build_folder_tree(self.datacenter.vmFolder)
        self.foldermap = self._build_folder_map(self.folders)

    def compile_folder_path_for_object(self, vobj):
        """ make a /vm/foo/bar/baz like folder path for an object """

        paths = []
        if isinstance(vobj, vim.Folder):
            paths.append(vobj.name)

        thisobj = vobj
        while hasattr(thisobj, 'parent'):
            thisobj = thisobj.parent
            if isinstance(thisobj, vim.Folder):
                paths.append(thisobj.name)
        paths.reverse()
        if paths[0] == 'Datacenters':
            paths.remove('Datacenters')
        return '/' + '/'.join(paths)

    def get_datacenter(self):
        self.datacenter = get_obj(self.content, [vim.Datacenter],
                                  self.params['datacenter'])

    def getvm(self, name=None, uuid=None, folder=None, name_match=None):

        # https://www.vmware.com/support/developer/vc-sdk/visdk2xpubs/ReferenceGuide/vim.SearchIndex.html
        # self.si.content.searchIndex.FindByInventoryPath('DC1/vm/test_folder')

        vm = None
        searchpath = None

        if uuid:
            vm = self.content.searchIndex.FindByUuid(uuid=uuid, vmSearch=True)

        elif folder:
            if self.params['folder'].endswith('/'):
                self.params['folder'] = self.params['folder'][0:-1]

            # Build the absolute folder path to pass into the search method
            if self.params['folder'].startswith('/vm'):
                searchpath = '%s' % self.params['datacenter']
                searchpath += self.params['folder']
            elif self.params['folder'].startswith('/'):
                searchpath = '%s' % self.params['datacenter']
                searchpath += '/vm' + self.params['folder']
            else:
                # need to look for matching absolute path
                if not self.folders:
                    self.getfolders()
                paths = self.foldermap['paths'].keys()
                paths = [x for x in paths if x.endswith(self.params['folder'])]
                if len(paths) > 1:
                    self.module.fail_json(
                        msg='%s matches more than one folder. Please use the absolute path starting with /vm/' %
                            self.params['folder'])
                elif paths:
                    searchpath = paths[0]

            if searchpath:
                # get all objects for this path ...
                fObj = self.content.searchIndex.FindByInventoryPath(searchpath)
                if fObj:
                    if isinstance(fObj, vim.Datacenter):
                        fObj = fObj.vmFolder
                    for cObj in fObj.childEntity:
                        if not isinstance(cObj, vim.VirtualMachine):
                            continue
                        if cObj.name == name:
                            vm = cObj
                            break

        if not vm:
            # FIXME - this is unused if folder has a default value

            # narrow down by folder
            if folder:
                if not self.folders:
                    self.getfolders()

                # compare the folder path of each VM against the search path
                vmList = get_all_objs(self.content, [vim.VirtualMachine])
                for item in vmList.items():
                    vobj = item[0]
                    if not isinstance(vobj.parent, vim.Folder):
                        continue
                    if self.compile_folder_path_for_object(vobj) == searchpath:
                        return vobj

            if name_match:
                if name_match == 'first':
                    vm = get_obj(self.content, [vim.VirtualMachine], name)
                elif name_match == 'last':
                    matches = []
                    for thisvm in get_all_objs(self.content, [vim.VirtualMachine]):
                        if thisvm.config.name == name:
                            matches.append(thisvm)
                    if matches:
                        vm = matches[-1]
            else:
                matches = []
                for thisvm in get_all_objs(self.content, [vim.VirtualMachine]):
                    if thisvm.config.name == name:
                        matches.append(thisvm)
                    if len(matches) > 1:
                        self.module.fail_json(
                            msg='more than 1 vm exists by the name %s. Please specify a uuid, or a folder, '
                                'or a datacenter or name_match' % name)
                    if matches:
                        vm = matches[0]

        return vm

    def set_powerstate(self, vm, state, force):
        """
        Set the power status for a VM determined by the current and
        requested states. force is forceful
        """
        facts = self.gather_facts(vm)
        expected_state = state.replace('_', '').lower()
        current_state = facts['hw_power_status'].lower()
        result = {}

        # Need Force
        if not force and current_state not in ['poweredon', 'poweredoff']:
            return "VM is in %s power state. Force is required!" % current_state

        # State is already true
        if current_state == expected_state:
            result['changed'] = False
            result['failed'] = False
        else:
            task = None
            try:
                if expected_state == 'poweredoff':
                    task = vm.PowerOff()

                elif expected_state == 'poweredon':
                    task = vm.PowerOn()

                elif expected_state == 'restarted':
                    if current_state in ('poweredon', 'poweringon', 'resetting'):
                        task = vm.Reset()
                    else:
                        result = {'changed': False, 'failed': True,
                                  'msg': "Cannot restart VM in the current state %s" % current_state}

            except Exception:
                result = {'changed': False, 'failed': True,
                          'msg': get_exception()}

            if task:
                self.wait_for_task(task)
                if task.info.state == 'error':
                    result = {'changed': False, 'failed': True, 'msg': task.info.error.msg}
                else:
                    result = {'changed': True, 'failed': False}

        # need to get new metadata if changed
        if result['changed']:
            newvm = self.getvm(uuid=vm.config.uuid)
            facts = self.gather_facts(newvm)
            result['instance'] = facts
        return result

    def gather_facts(self, vm):

        """ Gather facts from vim.VirtualMachine object. """

        facts = {
            'module_hw': True,
            'hw_name': vm.config.name,
            'hw_power_status': vm.summary.runtime.powerState,
            'hw_guest_full_name': vm.summary.guest.guestFullName,
            'hw_guest_id': vm.summary.guest.guestId,
            'hw_product_uuid': vm.config.uuid,
            'hw_processor_count': vm.config.hardware.numCPU,
            'hw_memtotal_mb': vm.config.hardware.memoryMB,
            'hw_interfaces': [],
            'ipv4': None,
            'ipv6': None,
        }

        netDict = {}
        for device in vm.guest.net:
            netDict[device.macAddress] = list(device.ipAddress)

        for k, v in iteritems(netDict):
            for ipaddress in v:
                if ipaddress:
                    if '::' in ipaddress:
                        facts['ipv6'] = ipaddress
                    else:
                        facts['ipv4'] = ipaddress

        for idx, entry in enumerate(vm.config.hardware.device):
            if not hasattr(entry, 'macAddress'):
                continue

            factname = 'hw_eth' + str(idx)
            facts[factname] = {
                'addresstype': entry.addressType,
                'label': entry.deviceInfo.label,
                'macaddress': entry.macAddress,
                'ipaddresses': netDict.get(entry.macAddress, None),
                'macaddress_dash': entry.macAddress.replace(':', '-'),
                'summary': entry.deviceInfo.summary,
            }
            facts['hw_interfaces'].append('eth' + str(idx))

        return facts

    def remove_vm(self, vm):
        # https://www.vmware.com/support/developer/converter-sdk/conv60_apireference/vim.ManagedEntity.html#destroy
        task = vm.Destroy()
        self.wait_for_task(task)

        if task.info.state == 'error':
            return {'changed': False, 'failed': True, 'msg': task.info.error.msg}
        else:
            return {'changed': True, 'failed': False}

    def configure_network(self, template):
        network_devices = list()
        for network in self.params['networks']:
            if network:
                if 'ip' not in self.params['networks'][network]:
                    self.module.fail_json(msg="ip attribute is missing for network %s" % network)

                ip = self.params['networks'][network]['ip']
                if ip not in IPNetwork(network):
                    self.module.fail_json(msg="ip '%s' not in network %s" % (ip, network))

                ipnet = IPNetwork(network)
                self.params['networks'][network]['subnet_mask'] = str(ipnet.netmask)

                if get_obj(self.content, [vim.Network], self.params['networks'][network]['network']) is None:
                    self.module.fail_json(msg="Network %s doesn't exists" % network)

                network_devices.append(self.params['networks'][network])

        adaptermaps = []

        for key in range(0, len(network_devices)):
            if self.is_template():
                try:
                    for device in template.config.hardware.device:
                        if hasattr(device, 'addressType'):
                            nic = vim.vm.device.VirtualDeviceSpec()
                            nic.operation = vim.vm.device.VirtualDeviceSpec.Operation.remove
                            nic.device = device
                            self.configspec.deviceChange.append(nic)
                except:
                    pass

            # Default device type is vmxnet3, VMWare best practice
            device_type = network_devices[key]['device_type'] \
                if 'device_type' in network_devices[key] else 'vmxnet3'

            nic = self.device_helper.create_nic(device_type,
                                                'Network Adapter %s' % (key + 1),
                                                network_devices[key]['network'])
            nic.operation = vim.vm.device.VirtualDeviceSpec.Operation.add

            if hasattr(get_obj(self.content, [vim.Network], network_devices[key]['network']), 'portKeys'):
                # VDS switch
                pg_obj = get_obj(self.content, [vim.dvs.DistributedVirtualPortgroup], network_devices[key]['network'])
                dvs_port_connection = vim.dvs.PortConnection()
                dvs_port_connection.portgroupKey = pg_obj.key
                dvs_port_connection.switchUuid = pg_obj.config.distributedVirtualSwitch.uuid
                nic.device.backing = vim.vm.device.VirtualEthernetCard.DistributedVirtualPortBackingInfo()
                nic.device.backing.port = dvs_port_connection
            else:
                # vSwitch
                nic.device.backing = vim.vm.device.VirtualEthernetCard.NetworkBackingInfo()
                nic.device.backing.network = get_obj(self.content, [vim.Network], network_devices[key]['network'])
                nic.device.backing.deviceName = network_devices[key]['network']

            self.configspec.deviceChange.append(nic)

            guest_map = vim.vm.customization.AdapterMapping()
            guest_map.adapter = vim.vm.customization.IPSettings()
            guest_map.adapter.ip = vim.vm.customization.FixedIp()
            guest_map.adapter.ip.ipAddress = str(network_devices[key]['ip'])
            guest_map.adapter.subnetMask = str(network_devices[key]['subnet_mask'])

            try:
                guest_map.adapter.gateway = network_devices[key]['gateway']
            except:
                pass

            try:
                guest_map.adapter.dnsDomain = self.params['domain']
            except:
                pass

            adaptermaps.append(guest_map)

        # DNS settings
        globalip = vim.vm.customization.GlobalIPSettings()
        globalip.dnsServerList = self.params['dns_servers']
        globalip.dnsSuffixList = str(self.params['domain'])

        # Hostname settings
        ident = vim.vm.customization.LinuxPrep()
        ident.domain = str(self.params['domain'])
        ident.hostName = vim.vm.customization.FixedName()
        ident.hostName.name = self.params['name']

        self.customspec = vim.vm.customization.Specification()
        self.customspec.nicSettingMap = adaptermaps
        self.customspec.globalIPSettings = globalip
        self.customspec.identity = ident

    def configure_disks(self, template):
        if self.is_template():
            # grab the template's first disk and modify it for this customization
            disks = [x for x in template.config.hardware.device if isinstance(x, vim.vm.device.VirtualDisk)]
            diskspec = vim.vm.device.VirtualDeviceSpec()
            # set the operation to edit so that it knows to keep other settings
            diskspec.operation = vim.vm.device.VirtualDeviceSpec.Operation.edit
            diskspec.device = disks[0]
        else:
            scsi_ctl = self.device_helper.create_scsi_controller()
            self.configspec.deviceChange.append(scsi_ctl)

            diskspec = self.device_helper.create_scsi_disk(scsi_ctl)
            # TODO change this ID when looping over disk array
            diskspec.device.unitNumber = 0

        # get the first disk attributes
        # @TODO loop over this to create multiple disks
        pspec = self.params.get('disk')[0]

        # is it thin?
        if pspec.get('type', '').lower() == 'thin':
            diskspec.device.backing.thinProvisioned = True

        # which datastore?
        if pspec.get('datastore'):
            # This is already handled by the relocation spec,
            # but it needs to eventually be handled for all the
            # other disks defined
            pass

        # what size is it?
        if [x for x in pspec.keys() if x.startswith('size_') or x == 'size']:
            # size_tb, size_gb, size_mb, size_kb, size_b ...?
            if 'size' in pspec:
                expected = ''.join(c for c in pspec['size'] if c.isdigit())
                unit = pspec['size'].replace(expected, '').lower()
                expected = int(expected)
            else:
                param = [x for x in pspec.keys() if x.startswith('size_')][0]
                unit = param.split('_')[-1].lower()
                expected = [x[1] for x in pspec.items() if x[0].startswith('size_')][0]
                expected = int(expected)

            kb = None
            if unit == 'tb':
                kb = expected * 1024 * 1024 * 1024
            elif unit == 'gb':
                kb = expected * 1024 * 1024
            elif unit == ' mb':
                kb = expected * 1024
            elif unit == 'kb':
                kb = expected
            else:
                self.module.fail_json(msg='%s is not a supported unit for disk size' % unit)
            diskspec.device.capacityInKB = kb

        return diskspec

    def deploy_template(self, wait_for_ip=False):

        # https://github.com/vmware/pyvmomi-community-samples/blob/master/samples/clone_vm.py
        # https://www.vmware.com/support/developer/vc-sdk/visdk25pubs/ReferenceGuide/vim.vm.CloneSpec.html
        # https://www.vmware.com/support/developer/vc-sdk/visdk25pubs/ReferenceGuide/vim.vm.ConfigSpec.html
        # https://www.vmware.com/support/developer/vc-sdk/visdk41pubs/ApiReference/vim.vm.RelocateSpec.html

        # FIXME:
        #   - clusters
        #   - multiple datacenters
        #   - resource pools
        #   - multiple templates by the same name
        #   - multiple disks
        #   - changing the esx host is ignored?
        #   - static IPs

        datacenters = get_all_objs(self.content, [vim.Datacenter])
        datacenter = get_obj(self.content, [vim.Datacenter], self.params['datacenter'])
        if not datacenter:
            self.module.fail_json(msg='No datacenter named %s was found' % self.params['datacenter'])

        if not self.foldermap:
            self.getfolders()

        # find matching folders
        if self.params['folder'].startswith('/'):
            folders = [x for x in self.foldermap['fvim_by_path'].items() if x[0] == self.params['folder']]
        else:
            folders = [x for x in self.foldermap['fvim_by_path'].items() if x[0].endswith(self.params['folder'])]

        # throw error if more than one match or no matches
        if len(folders) == 0:
            self.module.fail_json(msg='no folder matched the path: %s' % self.params['folder'])
        elif len(folders) > 1:
            self.module.fail_json(
                msg='too many folders matched "%s", please give the full path starting with /vm/' % self.params[
                    'folder'])

        # grab the folder vim object
        destfolder = folders[0][1]

        # if the user wants a cluster, get the list of hosts for the cluster and use the first one
        if self.params['cluster']:
            cluster = get_obj(self.content, [vim.ClusterComputeResource], self.params['cluster'])
            if not cluster:
                self.module.fail_json(msg="Failed to find a cluster named %s" % self.params['cluster'])
            # resource_pool = cluster.resourcePool
            hostsystems = [x for x in cluster.host]
            hostsystem = hostsystems[0]
        else:
            hostsystem = get_obj(self.content, [vim.HostSystem], self.params['esxi_hostname'])
            if not hostsystem:
                self.module.fail_json(msg="Failed to find a host named %s" % self.params['esxi_hostname'])

        # FIXME: need to search for this in the same way as guests to ensure accuracy
        if self.is_template():
            template = get_obj(self.content, [vim.VirtualMachine], self.params['template'])
            if not template:
                self.module.fail_json(msg="Could not find a template named %s" % self.params['template'])
        else:
            template = None

        # set the destination datastore in the relocation spec
        datastore_name = None
        datastore = None
        if self.params['disk']:
            if 'datastore' in self.params['disk'][0]:
                datastore_name = self.params['disk'][0]['datastore']
                datastore = get_obj(self.content, [vim.Datastore], datastore_name)
        if not datastore and self.is_template():
            # use the template's existing DS
            disks = [x for x in template.config.hardware.device if isinstance(x, vim.vm.device.VirtualDisk)]
            datastore = disks[0].backing.datastore
            datastore_name = datastore.name
        if not datastore:
            self.module.fail_json(msg="Failed to find a matching datastore")

        # create the relocation spec
        relospec = vim.vm.RelocateSpec()
        relospec.host = hostsystem
        relospec.datastore = datastore

        # Find the associated resourcepool for the host system
        #   * FIXME: find resourcepool for clusters too
        resource_pool = None
        resource_pools = get_all_objs(self.content, [vim.ResourcePool])
        for rp in resource_pools.items():
            if not rp[0]:
                continue
            if not hasattr(rp[0], 'parent'):
                continue
            if rp[0].parent == hostsystem.parent:
                resource_pool = rp[0]
                break
        if resource_pool:
            relospec.pool = resource_pool
        else:
            self.module.fail_json(msg="Failed to find a resource group for %s" % hostsystem.name)

        self.configspec = vim.vm.ConfigSpec(cpuHotAddEnabled=True, memoryHotAddEnabled=True)
        clonespec = vim.vm.CloneSpec(template=self.params['template_flag'],
                                     location=relospec)
        self.configspec.deviceChange = []

        # create disk spec if not default
        if self.params['disk']:
            # @TODO get all disks & loop like network cards
            # tell the configspec that the disk device needs to change
            self.configspec.deviceChange.append(self.configure_disks(template=template))

        # set cpu/memory/etc
        if 'hardware' in self.params:
            if 'num_cpus' in self.params['hardware']:
                self.configspec.numCPUs = int(self.params['hardware']['num_cpus'])
            # num_cpu is mandatory for VM creation
            elif not self.is_template():
                self.module.fail_json(msg="hardware.num_cpus attribute is mandatory for VM creation")

            if 'memory_mb' in self.params['hardware']:
                self.configspec.memoryMB = int(self.params['hardware']['memory_mb'])
            # memory_mb is mandatory for VM creation
            elif not self.is_template():
                self.module.fail_json(msg="hardware.memory_mb attribute is mandatory for VM creation")

        self.configure_network(template=template)
        if self.params['customize'] is True:
            clonespec.customization = self.customspec

        try:
            if self.is_template():
                clonespec.config = self.configspec
                task = template.Clone(folder=destfolder, name=self.params['name'], spec=clonespec)
            else:
                # ConfigSpec require name for VM creation
                self.configspec.name = self.params['name']
                self.configspec.files = vim.vm.FileInfo(logDirectory=None,
                                                        snapshotDirectory=None,
                                                        suspendDirectory=None,
                                                        vmPathName="[" + datastore_name + "] " + self.params["name"])

                task = destfolder.CreateVM_Task(config=self.configspec, pool=resource_pool)
            self.wait_for_task(task)
        except TypeError:
            self.module.fail_json(msg="TypeError was returned, please ensure to give correct inputs.")

        if task.info.state == 'error':
            # https://kb.vmware.com/selfservice/microsites/search.do?language=en_US&cmd=displayKC&externalId=2021361
            # https://kb.vmware.com/selfservice/microsites/search.do?language=en_US&cmd=displayKC&externalId=2173
            return {'changed': False, 'failed': True, 'msg': task.info.error.msg}
        else:
            # set annotation
            vm = task.info.result
            if self.params['annotation']:
                annotation_spec = vim.vm.ConfigSpec()
                annotation_spec.annotation = str(self.params['annotation'])
                task = vm.ReconfigVM_Task(annotation_spec)
                self.wait_for_task(task)

            if wait_for_ip:
                self.set_powerstate(vm, 'poweredon', force=False)
                self.wait_for_vm_ip(vm)

            vm_facts = self.gather_facts(vm)
            return {'changed': True, 'failed': False, 'instance': vm_facts}

    def wait_for_task(self, task):
        # https://www.vmware.com/support/developer/vc-sdk/visdk25pubs/ReferenceGuide/vim.Task.html
        # https://www.vmware.com/support/developer/vc-sdk/visdk25pubs/ReferenceGuide/vim.TaskInfo.html
        # https://github.com/virtdevninja/pyvmomi-community-samples/blob/master/samples/tools/tasks.py
        while task.info.state not in ['success', 'error']:
            time.sleep(1)

    def wait_for_vm_ip(self, vm, poll=100, sleep=5):
        ips = None
        facts = {}
        thispoll = 0
        while not ips and thispoll <= poll:
            newvm = self.getvm(uuid=vm.config.uuid)
            facts = self.gather_facts(newvm)
            if facts['ipv4'] or facts['ipv6']:
                ips = True
            else:
                time.sleep(sleep)
                thispoll += 1

        return facts

    def fetch_file_from_guest(self, vm, username, password, src, dest):

        """ Use VMWare's filemanager api to fetch a file over http """

        result = {'failed': False}

        tools_status = vm.guest.toolsStatus
        if tools_status == 'toolsNotInstalled' or tools_status == 'toolsNotRunning':
            result['failed'] = True
            result['msg'] = "VMwareTools is not installed or is not running in the guest"
            return result

        # https://github.com/vmware/pyvmomi/blob/master/docs/vim/vm/guest/NamePasswordAuthentication.rst
        creds = vim.vm.guest.NamePasswordAuthentication(
            username=username, password=password
        )

        # https://github.com/vmware/pyvmomi/blob/master/docs/vim/vm/guest/FileManager/FileTransferInformation.rst
        fti = self.content.guestOperationsManager.fileManager. \
            InitiateFileTransferFromGuest(vm, creds, src)

        result['size'] = fti.size
        result['url'] = fti.url

        # Use module_utils to fetch the remote url returned from the api
        rsp, info = fetch_url(self.module, fti.url, use_proxy=False,
                              force=True, last_mod_time=None,
                              timeout=10, headers=None)

        # save all of the transfer data
        for k, v in iteritems(info):
            result[k] = v

        # exit early if xfer failed
        if info['status'] != 200:
            result['failed'] = True
            return result

        # attempt to read the content and write it
        try:
            with open(dest, 'wb') as f:
                f.write(rsp.read())
        except Exception as e:
            result['failed'] = True
            result['msg'] = str(e)

        return result

    def push_file_to_guest(self, vm, username, password, src, dest, overwrite=True):

        """ Use VMWare's filemanager api to fetch a file over http """

        result = {'failed': False}

        tools_status = vm.guest.toolsStatus
        if tools_status == 'toolsNotInstalled' or tools_status == 'toolsNotRunning':
            result['failed'] = True
            result['msg'] = "VMwareTools is not installed or is not running in the guest"
            return result

        # https://github.com/vmware/pyvmomi/blob/master/docs/vim/vm/guest/NamePasswordAuthentication.rst
        creds = vim.vm.guest.NamePasswordAuthentication(
            username=username, password=password
        )

        # the api requires a filesize in bytes
        fdata = None
        try:
            # filesize = os.path.getsize(src)
            filesize = os.stat(src).st_size
            with open(src, 'rb') as f:
                fdata = f.read()
            result['local_filesize'] = filesize
        except Exception as e:
            result['failed'] = True
            result['msg'] = "Unable to read src file: %s" % str(e)
            return result

        # https://www.vmware.com/support/developer/converter-sdk/conv60_apireference/vim.vm.guest.FileManager.html#initiateFileTransferToGuest
        file_attribute = vim.vm.guest.FileManager.FileAttributes()
        url = self.content.guestOperationsManager.fileManager. \
            InitiateFileTransferToGuest(vm, creds, dest, file_attribute,
                                        filesize, overwrite)

        # PUT the filedata to the url ...
        rsp, info = fetch_url(self.module, url, method="put", data=fdata,
                              use_proxy=False, force=True, last_mod_time=None,
                              timeout=10, headers=None)

        result['msg'] = str(rsp.read())

        # save all of the transfer data
        for k, v in iteritems(info):
            result[k] = v

        return result

    def run_command_in_guest(self, vm, username, password, program_path, program_args, program_cwd, program_env):

        result = {'failed': False}

        tools_status = vm.guest.toolsStatus
        if (tools_status == 'toolsNotInstalled' or
                    tools_status == 'toolsNotRunning'):
            result['failed'] = True
            result['msg'] = "VMwareTools is not installed or is not running in the guest"
            return result

        # https://github.com/vmware/pyvmomi/blob/master/docs/vim/vm/guest/NamePasswordAuthentication.rst
        creds = vim.vm.guest.NamePasswordAuthentication(
            username=username, password=password
        )

        try:
            # https://github.com/vmware/pyvmomi/blob/master/docs/vim/vm/guest/ProcessManager.rst
            pm = self.content.guestOperationsManager.processManager
            # https://www.vmware.com/support/developer/converter-sdk/conv51_apireference/vim.vm.guest.ProcessManager.ProgramSpec.html
            ps = vim.vm.guest.ProcessManager.ProgramSpec(
                # programPath=program,
                # arguments=args
                programPath=program_path,
                arguments=program_args,
                workingDirectory=program_cwd,
            )

            res = pm.StartProgramInGuest(vm, creds, ps)
            result['pid'] = res
            pdata = pm.ListProcessesInGuest(vm, creds, [res])

            # wait for pid to finish
            while not pdata[0].endTime:
                time.sleep(1)
                pdata = pm.ListProcessesInGuest(vm, creds, [res])

            result['owner'] = pdata[0].owner
            result['startTime'] = pdata[0].startTime.isoformat()
            result['endTime'] = pdata[0].endTime.isoformat()
            result['exitCode'] = pdata[0].exitCode
            if result['exitCode'] != 0:
                result['failed'] = True
                result['msg'] = "program exited non-zero"
            else:
                result['msg'] = "program completed successfully"

        except Exception as e:
            result['msg'] = str(e)
            result['failed'] = True

        return result

    def list_snapshots_recursively(self, snapshots):
        snapshot_data = []
        for snapshot in snapshots:
            snap_text = 'Id: %s; Name: %s; Description: %s; CreateTime: %s; State: %s' % (snapshot.id, snapshot.name,
                                                                                          snapshot.description,
                                                                                          snapshot.createTime,
                                                                                          snapshot.state)
            snapshot_data.append(snap_text)
            snapshot_data = snapshot_data + self.list_snapshots_recursively(snapshot.childSnapshotList)
        return snapshot_data

    def get_snapshots_by_name_recursively(self, snapshots, snapname):
        snap_obj = []
        for snapshot in snapshots:
            if snapshot.name == snapname:
                snap_obj.append(snapshot)
            else:
                snap_obj = snap_obj + self.get_snapshots_by_name_recursively(snapshot.childSnapshotList, snapname)
        return snap_obj

    def get_current_snap_obj(self, snapshots, snapob):
        snap_obj = []
        for snapshot in snapshots:
            if snapshot.snapshot == snapob:
                snap_obj.append(snapshot)
            snap_obj = snap_obj + self.get_current_snap_obj(snapshot.childSnapshotList, snapob)
        return snap_obj

    def snapshot_vm(self, vm, guest, snapshot_op):
        """ To perform snapshot operations create/remove/revert/list_all/list_current/remove_all """

        snapshot_op_name = None
        try:
            snapshot_op_name = snapshot_op['op_type']
        except KeyError:
            self.module.fail_json(msg="Specify op_type - create/remove/revert/list_all/list_current/remove_all")

        task = None
        result = {}

        if snapshot_op_name not in ['create', 'remove', 'revert', 'list_all', 'list_current', 'remove_all']:
            self.module.fail_json(msg="Specify op_type - create/remove/revert/list_all/list_current/remove_all")

        if snapshot_op_name != 'create' and vm.snapshot is None:
            self.module.exit_json(msg="VM - %s doesn't have any snapshots" % guest)

        if snapshot_op_name == 'create':
            try:
                snapname = snapshot_op['name']
            except KeyError:
                self.module.fail_json(msg="specify name & description(optional) to create a snapshot")

            if 'description' in snapshot_op:
                snapdesc = snapshot_op['description']
            else:
                snapdesc = ''

            dumpMemory = False
            quiesce = False
            task = vm.CreateSnapshot(snapname, snapdesc, dumpMemory, quiesce)

        elif snapshot_op_name in ['remove', 'revert']:
            try:
                snapname = snapshot_op['name']
            except KeyError:
                self.module.fail_json(msg="specify snapshot name")

            snap_obj = self.get_snapshots_by_name_recursively(vm.snapshot.rootSnapshotList, snapname)

            # if len(snap_obj) is 0; then no snapshots with specified name
            if len(snap_obj) == 1:
                snap_obj = snap_obj[0].snapshot
                if snapshot_op_name == 'remove':
                    task = snap_obj.RemoveSnapshot_Task(True)
                else:
                    task = snap_obj.RevertToSnapshot_Task()
            else:
                self.module.exit_json(
                    msg="Couldn't find any snapshots with specified name: %s on VM: %s" % (snapname, guest))

        elif snapshot_op_name == 'list_all':
            snapshot_data = self.list_snapshots_recursively(vm.snapshot.rootSnapshotList)
            result['snapshot_data'] = snapshot_data

        elif snapshot_op_name == 'list_current':
            current_snapref = vm.snapshot.currentSnapshot
            current_snap_obj = self.get_current_snap_obj(vm.snapshot.rootSnapshotList, current_snapref)
            result['current_snapshot'] = 'Id: %s; Name: %s; Description: %s; CreateTime: %s; State: %s' % (
                current_snap_obj[0].id,
                current_snap_obj[0].name, current_snap_obj[0].description, current_snap_obj[0].createTime,
                current_snap_obj[0].state)

        elif snapshot_op_name == 'remove_all':
            task = vm.RemoveAllSnapshots()

        if task:
            self.wait_for_task(task)
            if task.info.state == 'error':
                result = {'changed': False, 'failed': True, 'msg': task.info.error.msg}
            else:
                result = {'changed': True, 'failed': False}

        return result


def get_obj(content, vimtype, name):
    """
    Return an object by name, if name is None the
    first found object is returned
    """
    obj = None
    container = content.viewManager.CreateContainerView(
        content.rootFolder, vimtype, True)
    for c in container.view:
        if name:
            if c.name == name:
                obj = c
                break
        else:
            obj = c
            break

    container.Destroy()
    return obj


def main():
    module = AnsibleModule(
        argument_spec=dict(
            hostname=dict(
                type='str',
                default=os.environ.get('VMWARE_HOST')
            ),
            username=dict(
                type='str',
                default=os.environ.get('VMWARE_USER')
            ),
            password=dict(
                type='str', no_log=True,
                default=os.environ.get('VMWARE_PASSWORD')
            ),
            state=dict(
                required=False,
                choices=[
                    'poweredon',
                    'poweredoff',
                    'present',
                    'absent',
                    'restarted',
                    'reconfigured'
                ],
                default='present'),
            validate_certs=dict(required=False, type='bool', default=True),
            template_src=dict(required=False, type='str', aliases=['template'], default=None),
            template_flag=dict(required=False, type='bool', default=False),
            annotation=dict(required=False, type='str', aliases=['notes']),
            name=dict(required=True, type='str'),
            name_match=dict(required=False, type='str', default='first'),
            snapshot_op=dict(required=False, type='dict', default={}),
            uuid=dict(required=False, type='str'),
            folder=dict(required=False, type='str', default='/vm', aliases=['folder']),
            disk=dict(required=False, type='list'),
            nic=dict(required=False, type='list'),
            hardware=dict(required=False, type='dict', default={}),
            force=dict(required=False, type='bool', default=False),
            datacenter=dict(required=False, type='str', default=None),
            esxi_hostname=dict(required=False, type='str', default=None),
            cluster=dict(required=False, type='str', default=None),
            wait_for_ip_address=dict(required=False, type='bool', default=True),
            customize=dict(required=False, type='bool', default=False),
            dns_servers=dict(required=False, type='list', default=None),
            domain=dict(required=False, type='str', default=None),
            networks=dict(required=False, type='dict', default={})
        ),
        supports_check_mode=True,
        mutually_exclusive=[],
        required_together=[
            ['state', 'force'],
            ['template'],
        ],
    )

    result = {'failed': False, 'changed': True}

    pyv = PyVmomiHelper(module)
    # Check if the VM exists before continuing
    vm = pyv.getvm(name=module.params['name'],
                   folder=module.params['folder'],
                   uuid=module.params['uuid'],
                   name_match=module.params['name_match'])

    # VM already exists
    if vm:
        if module.params['state'] == 'absent':
            # destroy it
            if module.params['force']:
                # has to be poweredoff first
                pyv.set_powerstate(vm, 'poweredoff', module.params['force'])
            result = pyv.remove_vm(vm)
        elif module.params['state'] in ['poweredon', 'poweredoff', 'restarted']:
            # set powerstate
            result = pyv.set_powerstate(vm, module.params['state'], module.params['force'])
        elif module.params['snapshot_op']:
            result = pyv.snapshot_vm(vm, module.params['name'], module.params['snapshot_op'])
        else:
            # Run for facts only
            try:
                module.exit_json(instance=pyv.gather_facts(vm))
            except Exception:
                e = get_exception()
                module.fail_json(msg="Fact gather failed with exception %s" % e)

    # VM doesn't exist
    else:
        create_states = ['poweredon', 'poweredoff', 'present', 'restarted']
        if module.params['state'] in create_states:
            # Create it ...
            result = pyv.deploy_template(
                wait_for_ip=module.params['wait_for_ip_address']
            )
            result['changed'] = True

    if 'failed' not in result:
        result['failed'] = False

    if result['failed']:
        module.fail_json(**result)
    else:
        module.exit_json(**result)


from ansible.module_utils.vmware import *
from ansible.module_utils.basic import *

if __name__ == '__main__':
    main()
