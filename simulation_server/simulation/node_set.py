"""
base.py - base classes for system specific modules
"""
from abc import ABC
from ClusterShell.NodeSet import NodeSet
import re


class XnameMapper:
    """ Class that maps hostnames to xnames """
    def __init__(self,
        system: str, xnames_pattern: str, 
        hostname_nid_start: int, hostname_nid_digits: int,
    ):
        self.system = system
        self.xnames_pattern = xnames_pattern
        self.hostname_nid_start = hostname_nid_start
        self.hostname_nid_digits = hostname_nid_digits
        
        self._hostname_regex = re.compile(rf'^{system}(\d+)$')
        xname_node_set = NodeSet(xnames_pattern)
        self._host_nid_to_xname = {
            i + self.hostname_nid_start: xname
            for i, xname in enumerate(xname_node_set)
        }
        self._xname_to_host_nid = {v: k for k, v in self._host_nid_to_xname.items()}


    def hostname_to_xname(self, hostname: str):
        match = self._hostname_regex.match(hostname)
        xname = None
        if match:
            nid = int(match.group(1))
            xname = self._host_nid_to_xname.get(nid)
        
        if not xname:
            raise ValueError(f'hostname "{hostname}" is not valid')
        return xname


    def xname_to_hostname(self, xname: str):
        nid = self._xname_to_host_nid.get(xname)
        if nid is None:
            raise ValueError(f'xname "{xname}" is not valid')
        return f'{self.system}{nid:0{self.hostname_nid_digits}}'



class SystemNodeSet(NodeSet, ABC):
    """
    SystemNodes - a NodeSet with system specific settings to handle xnames.
    """
    # Set this in child classes
    xname_mapper: XnameMapper

    def hostnames(self) -> list[str]:
        """ Return a list of hostnames """
        return [n for n in self]


    @classmethod
    def hostname_to_xname(cls, xname: str):
        return cls.xname_mapper.hostname_to_xname(xname)


    @classmethod
    def xname_to_hostname(cls, hostname: str):
        return cls.xname_mapper.xname_to_hostname(hostname)


    def xnames(self) -> list[str]:
        """ Return a list of xnames """
        return [self.xname_mapper.hostname_to_xname(hostname) for hostname in self]


class FrontierNodeSet(SystemNodeSet):
    xname_mapper = XnameMapper(
        system = 'frontier',
        xnames_pattern = 'x[2000-2011,2100-2111,2200-2211,2300-2304,2306-2311,2400-2411,2500-2511,2600-2604,2606-2611]c[0-7]s[0-7]b[0-1]',
        hostname_nid_start = 1, hostname_nid_digits = 5,
    )
