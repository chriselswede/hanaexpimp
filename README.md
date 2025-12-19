# HANAExpImp
The HANA exporter could help to follow the SAP Note 3568889 in the case of not enough memory to export the full table.
This script could then help to export views in parts, sleep between each export and then import the data back into the table.
The views must be created before running this script.
It is assumed that the views are named as <view_name>\_1, <view_name>\_2, ..., <view_name>\_<number_views>

# Disclaimer
ANY USAGE OF HANAEXPIMP ASSUMES THAT YOU HAVE UNDERSTOOD AND AGREED THAT:
1. HANAExpImp is NOT SAP official software, so normal SAP support of hanaexpimp cannot be assumed  
2. HANAExpImp is open source     
3. HANAExpImp is provided "as is"  
4. HANAExpImp is to be used on "your own risk
