#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import sys
import urllib
import json

def generate_ignored_strings():
    ignored_prefixes = ["fax", "tel", "e-mail", "email", "internet"]
    ignored_sufixes = [" ", ".", ":"]
    global ignored_strings
    ignored_strings = []
    for pref in ignored_prefixes:
        for suf in ignored_sufixes:
            ignored_strings.append(pref + suf)
            
def contains_ignored_strings(string_to_test):
    lower_string_to_test = string_to_test.lower();
    for ignored_string in ignored_strings:
        if ignored_string in lower_string_to_test : return True
    return False

def parse_addr(row_id, addr_string_array):    

    addr_string_array = remove_tel_fax_etc(addr_string_array)
    if constains_arrors(row_id, addr_string_array):
        row_tulpe = (row_id, addr_string_array)
        rows_with_error.append(row_tulpe)
    else:
        build_nominatim_url(row_id, addr_string_array)

def constains_arrors(row_id, addr_string_array):
    if addr_string_array[-1].upper() != addr_string_array[-1]:
        return True
    if len(addr_string_array) < 2:
        return True
    return False

def build_nominatim_url(row_id, addr_string_array):
    address = addr_string_array[-2]
    country = addr_string_array[-1]
    url = 'http://open.mapquestapi.com/nominatim/v1/search.php?format=json&addressdetails=1&q='
    url += country + '+'
    url += address.replace(' ', '+')
    response = urllib.urlopen(url)
    json_resp = response.read()
    json_dict = json.loads(json_resp)
    if len(json_dict) < 1:
        row_tulpe = (row_id, addr_string_array)
        rows_with_error.append(row_tulpe)
    else:
        print '-------------------------------- ', row_id
        print json_dict[0]
#        print json_dict[0]['display_name']
    
def remove_tel_fax_etc(addr_string_array):
    new_addr_string_array = []
    for line in addr_string_array:
        if not contains_ignored_strings(line):
            new_addr_string_array.append(line)
    return new_addr_string_array

con = None
ignored_strings = None
rows_with_error = []
print_errors = True

generate_ignored_strings()

# print ignored_strings

try:
     
    con = psycopg2.connect(database='dataharvest2', user='andrei') 
    cur = con.cursor()
    cur.execute('select id, tenderer_addr from awards LIMIT 10')          
    rows = cur.fetchall()
    print "All rows", len(rows)
    for tenderer_addr in rows:
        row_id = tenderer_addr[0]
        if tenderer_addr[1]:
            parse_addr(row_id, tenderer_addr[1].split('\n'))

    if print_errors and rows_with_error != None:
        print "Rows with error ", len(rows_with_error)
        for row_with_error in rows_with_error:
            print '-------------------------------- id=', row_with_error[0]
            print row_with_error[1]

except psycopg2.DatabaseError, e:
    print 'Error %s' % e    
    sys.exit(1)
    
finally:
    
    if con:
        con.close()
