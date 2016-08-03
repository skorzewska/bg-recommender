import xml.etree.ElementTree as ET


"""Parse single game XML file"""

filename = "users/Adbet.xml"

tree = ET.parse(filename)
items = tree.getroot()
for item in items.findall("item"):
    print item.find("stats").find("rating").get("value")


