"""Synthetic fixtures follow the official NiFi 1.x schemas referenced in nifi_xml.

Names, IDs, processors and values here are synthetic; no production flow data.
"""
import unittest
from flowbridge.nifi_xml import MAX_BYTES, parse_nifi_xml

TEMPLATE = b'''<?xml version="1.0" encoding="UTF-8"?>
<template encoding-version="1.3"><name>Example</name><snippet>
<processors><id>source</id><name>Read</name><type>org.example.Read</type>
<bundle><group>org.example</group><artifact>read-nar</artifact><version>1.0</version></bundle>
<config><properties><entry><key>topic</key><value>events</value></entry></properties>
<concurrentlySchedulableTaskCount>1</concurrentlySchedulableTaskCount></config></processors>
<processGroups><id>nested</id><name>Child</name><contents>
<inputPorts><id>port</id><name>In</name><type>INPUT_PORT</type></inputPorts>
<controllerServices><id>svc</id><type>org.example.Service</type><properties>
<entry><key>endpoint</key><value>example</value></entry></properties></controllerServices>
</contents></processGroups>
<connections><id>edge</id><source><id>source</id><groupId>root</groupId><type>PROCESSOR</type></source>
<destination><id>port</id><groupId>nested</groupId><type>INPUT_PORT</type></destination>
<selectedRelationships>success</selectedRelationships><selectedRelationships>retry</selectedRelationships>
</connections></snippet></template>'''

CONTROLLER = b'''<flowController><maxThreadCount>100</maxThreadCount><rootGroup>
<id>root</id><name>Root</name><processGroup><id>nested</id><name>Child</name>
<processor><id>read</id><class>org.example.Reader</class><maxConcurrentTasks>3</maxConcurrentTasks>
<property><name>path</name><value>/synthetic</value></property><running>true</running></processor>
<outputPort><id>out</id><name>Out</name></outputPort>
<connection><id>edge</id><sourceId>read</sourceId><sourceGroupId>nested</sourceGroupId>
<sourceType>PROCESSOR</sourceType><destinationId>out</destinationId>
<destinationGroupId>nested</destinationGroupId><destinationType>OUTPUT_PORT</destinationType>
<relationship>success</relationship></connection></processGroup></rootGroup></flowController>'''


class LegacyXMLTests(unittest.TestCase):
    def test_template_groups_services_edges(self):
        doc=parse_nifi_xml(TEMPLATE)
        group=doc['flowContents']
        self.assertEqual(group['name'],'Example')
        self.assertEqual(group['processors'][0]['properties'],{'topic':'events'})
        self.assertEqual(group['processors'][0]['bundle']['artifact'],'read-nar')
        self.assertEqual(group['processors'][0]['concurrentlySchedulableTaskCount'],1)
        child=group['processGroups'][0]
        self.assertEqual(child['controllerServices'][0]['properties']['endpoint'],'example')
        self.assertEqual(child['inputPorts'][0]['identifier'],'port')
        self.assertEqual(group['connections'][0]['destination']['groupId'],'nested')
        self.assertEqual(group['connections'][0]['selectedRelationships'],['success','retry'])
        self.assertTrue(doc['_flowbridge_xml']['export_blocked'])

    def test_persisted_flow_shape(self):
        doc=parse_nifi_xml(CONTROLLER)
        group=doc['flowContents']['processGroups'][0]
        self.assertEqual(group['processors'][0]['type'],'org.example.Reader')
        self.assertEqual(group['processors'][0]['properties']['path'],'/synthetic')
        self.assertEqual(group['processors'][0]['concurrentlySchedulableTaskCount'],3)
        self.assertEqual(group['connections'][0]['source'],{'id':'read','groupId':'nested','type':'PROCESSOR'})
        self.assertEqual(group['connections'][0]['selectedRelationships'],['success'])
        self.assertTrue(any('maxThreadCount' in p for p in doc['_flowbridge_xml']['unmapped_paths']))
        self.assertTrue(any('/running' in p for p in doc['_flowbridge_xml']['unmapped_paths']))

    def test_unknown_fields_attributes_and_text_retained(self):
        doc=parse_nifi_xml('<template><snippet><mystery flavor="new">value<child>more</child>tail</mystery></snippet></template>')
        tree=doc['_flowbridge_xml']['original_tree']['children'][0]['children'][0]
        self.assertEqual(tree['attributes'],{'flavor':'new'})
        self.assertEqual(tree['text'],'value')
        self.assertEqual(tree['children'][0]['tail'],'tail')
        self.assertTrue(doc['_flowbridge_xml']['unmapped_paths'])
        self.assertTrue(doc['_flowbridge_xml']['review_required'])

    def test_empty_missing_and_nil_property_values(self):
        doc=parse_nifi_xml('''<flowController xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><rootGroup><processor><id>x</id>
        <property><name>empty</name><value/></property><property><name>missing</name></property>
        <property><name>nil</name><value xsi:nil="true"/></property></processor></rootGroup></flowController>''')
        self.assertEqual(doc['flowContents']['processors'][0]['properties'],{'empty':'','missing':None,'nil':None})

    def test_dtd_entities_and_utf16_rejected(self):
        for data in [b'<!DOCTYPE template [<!ENTITY x SYSTEM "file:///etc/passwd">]><template><snippet>&x;</snippet></template>',
                     b'<!ENTITY x "data"><template><snippet/></template>',
                     '<template><snippet/></template>'.encode('utf-16')]:
            with self.assertRaises(ValueError): parse_nifi_xml(data)

    def test_size_depth_and_element_limits(self):
        for data in [b' '*(MAX_BYTES+1),b'<x>'*66+b'</x>'*66,
                     b'<template><snippet>'+b'<x/>'*20001+b'</snippet></template>']:
            with self.assertRaises(ValueError): parse_nifi_xml(data)

    def test_duplicates_are_not_silently_overwritten(self):
        for data in ['<template><snippet/><snippet/></template>',
                     '<template><snippet><processors><id>x</id><id>y</id></processors></snippet></template>',
                     '<template><snippet><processors><config><properties><entry><key>x</key><value>1</value></entry><entry><key>x</key><value>2</value></entry></properties></config></processors></snippet></template>']:
            with self.assertRaises(ValueError): parse_nifi_xml(data)

    def test_no_namespace_root_or_unknown_schema(self):
        for data in ['<template xmlns="urn:other"><snippet/></template>','<other/>','<template/>','<flowController/>']:
            with self.assertRaises(ValueError): parse_nifi_xml(data)

    def test_xml_escaped_text_is_data(self):
        doc=parse_nifi_xml('<template><snippet><processors><id>x</id><properties><entry><key>x</key><value>&lt;unsafe&gt;&amp;</value></entry></properties></processors></snippet></template>')
        self.assertEqual(doc['flowContents']['processors'][0]['properties']['x'],'<unsafe>&')

    def test_global_controller_services_keep_scope(self):
        doc=parse_nifi_xml('<flowController><rootGroup><id>root</id></rootGroup><controllerServices><controllerService><id>service</id><class>org.example.Service</class><property><name>x</name><value>y</value></property></controllerService></controllerServices></flowController>')
        self.assertEqual(doc['controllerServices'][0]['identifier'],'service')
        self.assertEqual(doc['controllerServices'][0]['properties'],{'x':'y'})
        self.assertEqual(doc['flowContents']['controllerServices'],[])
