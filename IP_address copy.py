#!/usr/bin/env python
# coding: utf-8

# In[1]:


import ipaddress
import binascii
import socket,struct
import pandas as pd
import numpy as np
import os


# In[2]:


os.getcwd()


# In[3]:


df_block = pd.read_csv ("./GeoIP2-City-Blocks-IPv4.csv",usecols=[0,1,2])
df_location= pd.read_csv ("./GeoIP2-City-Locations-en.csv",usecols=[0,3,5,10])


# In[4]:


df_block.count()


# In[5]:


df_location.count()


# In[6]:


df_block['geoname_id'].isna().sum()


# In[21]:


na_df=df_block[df_block['geoname_id'].isna()]


# In[22]:


na_df


# In[18]:


subnetwork_to_ip_range("103.151.145.10/32") #(103.151.145.10, 103.151.145.10)


# In[7]:


def subnetwork_to_ip_range(subnetwork):
 
    """
    Returns a tuple (ip_lower, ip_upper, version) containing the
    integer values of the lower and upper IP addresses respectively
    in a subnetwork expressed in CIDR notation (as a string), with
    version being the subnetwork IP version (either 4 or 6).
 
    Both IPv4 subnetworks (e.g. "192.168.1.0/24") and IPv6
    subnetworks (e.g. "2a02:a448:ddb0::/44") are accepted.
    """
 
    try:
        fragments = subnetwork.split('/')
        network_prefix = fragments[0]
        netmask_len = int(fragments[1])
 
        # try parsing the subnetwork first as IPv4, then as IPv6
        for version in (socket.AF_INET, socket.AF_INET6):
 
            ip_len = 32 if version == socket.AF_INET else 128
 
            try:
                suffix_mask = (1 << (ip_len - netmask_len)) - 1
                netmask = ((1 << ip_len) - 1) - suffix_mask
                ip_hex = socket.inet_pton(version, network_prefix)
                ip_lower = int(binascii.hexlify(ip_hex), 16) & netmask
                ip_upper = ip_lower + suffix_mask
 
                return (ip_lower,
                        ip_upper)
                        #4 if version == socket.AF_INET else 6)
            except:
                pass
    except:
        pass
 
    raise ValueError("invalid subnetwork")
#subnetwork_to_ip_range("192.0.2.16/29")
# def ip_address(netmaskip):
#     if type(netmaskip) != str:
#         return False
#     else:
#         min_ip,max_ip, v4orv6 =subnetwork_to_ip_range(netmaskip)
#         min_ip=socket.inet_ntoa(struct.pack('!L', min_ip))
#         max_ip= socket.inet_ntoa(struct.pack('!L', max_ip))
#     return min_ip, max_ip


# In[8]:


ip_range=[]
for i in df_block['network']:
    ip_range.append(subnetwork_to_ip_range(i))
    #df_block['low_ip'],df_block['high_ip']=(subnetwork_to_ip_range(i))
df_ip = pd.DataFrame(ip_range, columns=['low_ip','high_ip'])


# In[9]:


df_block.geoname_id.fillna(value=df_block['registered_country_geoname_id'], inplace=True)


# In[10]:


df_ip['geoname_id']=df_block['geoname_id']
print(df_ip.count())
df_ip['geoname_id'].isna().sum()


# In[11]:


df_ip.nunique()


# In[12]:


df_location.nunique()


# In[13]:


df_location['geoname_id'].isna().sum()


# In[14]:


df = pd.merge(df_ip, df_location, on=["geoname_id"])
df


# In[15]:


df.to_csv('ip_pandas.csv')

