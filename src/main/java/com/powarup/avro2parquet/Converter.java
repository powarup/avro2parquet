package com.powarup.avro2parquet;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import org.apache.parquet.avro.*;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Converter
{
	
	public static final CompressionCodecName defaultCompressionCodecName = CompressionCodecName.SNAPPY;
	
	public static void convert(String schemaFileName, String avroFileName, String parquetFileName) throws IOException {
		// to convert with default compression codec
		convert(schemaFileName, avroFileName, parquetFileName, defaultCompressionCodecName);
	}
	
	public static void convert(String schemaFileName, String avroFileName, String parquetFileName, CompressionCodecName compressionCodecName) throws IOException {
		Schema schema = new Parser().parse(new File(schemaFileName));

		File f = new File(parquetFileName);
		if (f.exists()) {
			f.delete();
		}
		Path outputPath = new Path(parquetFileName);

		// the ParquetWriter object that will consume Avro GenericRecords

		ParquetWriter<GenericRecord> parquetWriter = AvroParquetWriter
				.<GenericRecord>builder(outputPath)
				.withSchema(schema)
				.withConf(new Configuration())
				.withCompressionCodec(compressionCodecName)
				.build(); 
		
		// read GenericRecords from Avro file and write

		DataFileReader<GenericRecord> reader = new DataFileReader<GenericRecord>(new File(avroFileName), new GenericDatumReader<GenericRecord>());
		while (reader.hasNext()) {
			GenericRecord record = reader.next();
			parquetWriter.write(record);
		}

		parquetWriter.close();
		reader.close();
	}
	
	public static void main( String[] args )
	{
		// call with arguments: <optional>
		// path to Avro schema<.avsc>
		// path to Avro file<.avro>
		// <path to output Parquet file<.parquet>>
		
		String schemaFileName = args[0];
		if (!schemaFileName.endsWith(".avsc")) { // get filename for Avro schema
			schemaFileName += ".avsc";
		}
		String avroFileName = args[1];
		String parquetFileName = "";
		
		if (args.length > 2) {
			parquetFileName = args[2];
			if (!parquetFileName.endsWith(".parquet")) parquetFileName += ".parquet";
		}
		
		if (avroFileName.endsWith(".avro")) {
			int index = avroFileName.lastIndexOf(".avro");
			if (parquetFileName.isEmpty()) parquetFileName = avroFileName.substring(0, index) + ".parquet";
		} else {
			if (parquetFileName.isEmpty()) parquetFileName = avroFileName + ".parquet";
			avroFileName += ".avro";
		}
		
		try {
			convert(schemaFileName, avroFileName, parquetFileName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
