#!/bin/bash

# ###########################################################################################################
# Usage : Use this script while copying files in HDFS
# 1) Enter Source File Paths in "src_dir_array"
# 2) Enter Destination File Paths in "dst_dir_array"
# 3) Enter Source File Name Patterns in "filename". Leave it as-is if it matches your criteria
# 4) cd <directory where fileCopyHDFS.sh is placed>
# 5) ./fileCopyHDFSToHDFS.sh
# ###########################################################################################################

# put the source directory names in below array , within double quotes and seperated by blank space
src_dir_array=( "/home/edureka/POC/Spark/output/Customer_Complaint_Count_In_Category_In_Year_Month" "/home/edureka/POC/Spark/output/Customer_Complaint_Count_In_Category_In_Year_Month_Vs_Concented_Count" )

# put the destination directory names in below array , within double quotes and seperated by blank space
# IMPORTANT : make sure the number of source directories match number of destination directories
dst_dir_array=( "/home/edureka/Desktop/Test_Folder" "/home/edureka/Desktop/Test_Folder" )

# Enter File(s) to copy. Part-nnnn files by default
filename="part-*" 


# ###########################################################################################################
# CAUTION : DO NOT CHANGE BELOW CONTENT UNLESS NECESSARY
# ###########################################################################################################

echo "File Transfer Started...."

i=0

for element in ${src_dir_array[@]}
do
    	hadoop fs -cd $element
	for f in $filename
	do 
		hadoop fs -cp $f ${dst_dir_array[i]}/$f-$i-$(date +%m%d%y%H%M%S)
	done

    	i=$((i+1))

done

echo "File Transfer Complete...."
