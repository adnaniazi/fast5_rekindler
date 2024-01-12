import pysam
import csv
sampath="/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/ont_standards_processed/1_basecalled/RNA004/calls.sam"

outputcsv_path="/export/valenfs/data/processed_data/MinION/10_tailfindr_r10/ont_standards_processed/3_tailfindr/RNA004/onttails.csv"

samfile = pysam.AlignmentFile(sampath, 'r')

with open(outputcsv_path, 'w', newline='') as csvfile:
	csvwriter = csv.writer(csvfile)
	# Write header
	csvwriter.writerow(['read_id', 'ont_tail_length'])

	for read in samfile.fetch():	
		pt_tag_value = read.get_tag('pt') if read.has_tag('pt') else 0
		csvwriter.writerow([read.query_name, pt_tag_value])

samfile.close()