/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.benchmarks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static java.util.UUID.randomUUID;
import static org.apache.parquet.benchmarks.BenchmarkUtils.deleteIfExists;
import static org.apache.parquet.benchmarks.BenchmarkUtils.exists;
import static org.apache.parquet.column.ParquetProperties.WriterVersion;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.GZIP;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.apache.parquet.benchmarks.BenchmarkConstants.*;
import static org.apache.parquet.benchmarks.BenchmarkFiles.*;

public class DataGenerator {

  public void generateAll() {
	  ParquetProperties.WriterVersion version = WriterVersion.PARQUET_1_0;
    try {
      generateData(file_1M, configuration, version, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, ONE_MILLION);

      //generate data for different block and page sizes
      generateData(file_1M_BS256M_PS4M, configuration, version, BLOCK_SIZE_256M, PAGE_SIZE_4M, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, ONE_MILLION);
      generateData(file_1M_BS256M_PS8M, configuration, version, BLOCK_SIZE_256M, PAGE_SIZE_8M, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, ONE_MILLION);
      generateData(file_1M_BS512M_PS4M, configuration, version, BLOCK_SIZE_512M, PAGE_SIZE_4M, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, ONE_MILLION);
      generateData(file_1M_BS512M_PS8M, configuration, version, BLOCK_SIZE_512M, PAGE_SIZE_8M, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, ONE_MILLION);

      //generate data for different codecs
//      generateData(parquetFile_1M_LZO, configuration, PARQUET_2_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, FIXED_LEN_BYTEARRAY_SIZE, LZO, ONE_MILLION);
      generateData(file_1M_SNAPPY, configuration, version, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, FIXED_LEN_BYTEARRAY_SIZE, SNAPPY, ONE_MILLION);
      generateData(file_1M_GZIP, configuration, version, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, FIXED_LEN_BYTEARRAY_SIZE, GZIP, ONE_MILLION);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void generateHaggAll(int nGroups, int nRows) {
	  ParquetProperties.WriterVersion version = WriterVersion.PARQUET_1_0;
    try {
      generateHaggData(file_1M, configuration, version, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, nRows, nGroups);

      //generate data for different block and page sizes
      //generateHaggData(file_1M_BS256M_PS4M, configuration, version, BLOCK_SIZE_256M, PAGE_SIZE_4M, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, nRows, nGroups);
      //generateHaggData(file_1M_BS256M_PS8M, configuration, version, BLOCK_SIZE_256M, PAGE_SIZE_8M, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, nRows, nGroups);
      //generateHaggData(file_1M_BS512M_PS4M, configuration, version, BLOCK_SIZE_512M, PAGE_SIZE_4M, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, nRows, nGroups);
      //generateHaggData(file_1M_BS512M_PS8M, configuration, version, BLOCK_SIZE_512M, PAGE_SIZE_8M, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, nRows, nGroups);

      //generate data for different codecs
      //generateData(parquetFile_1M_LZO, configuration, PARQUET_2_0, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, FIXED_LEN_BYTEARRAY_SIZE, LZO, ONE_MILLION);
      //generateHaggData(file_1M_SNAPPY, configuration, version, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, FIXED_LEN_BYTEARRAY_SIZE, SNAPPY, nRows, nGroups);
      //generateHaggData(file_1M_GZIP, configuration, version, BLOCK_SIZE_DEFAULT, PAGE_SIZE_DEFAULT, FIXED_LEN_BYTEARRAY_SIZE, GZIP, nRows, nGroups);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }  
  public void generateHaggData(Path outFile, Configuration configuration, ParquetProperties.WriterVersion version,
		  int blockSize, int pageSize, int fixedLenByteArraySize, CompressionCodecName codec, int nRows, int nGroups) throws IOException
  {
	MessageType schema = parseMessageType(
	     "message test { "
	                    + "required int64 row_count; "
	                    + "required int32 gby_int32; "
	                    + "required int32 gby_int32_rand; "
	                    + "required binary gby_string (UTF8);"
	                    + "required float gby_float; "
	                    + "required int32 gby_date (DATE); "
	                    + "required int64 gby_timestamp (TIMESTAMP_MILLIS); "
	                    + "required binary gby_same (UTF8); "
	                    + "optional binary gby_rand (UTF8); "
	                    + "required int32 int32_field; "
	                    + "required int64 int64_field; "
	                    + "required int64 int64_rand; "
	                    + "required boolean boolean_field; "
	                    + "required float float_field; "
	                    + "required float float_rand; "
	                    + "required double double_field; "
	                    + "required double double_rand; "
	                    + "required fixed_len_byte_array(" + fixedLenByteArraySize +") flba_field; "
	                    + "required int96 int96_field; "
	                    + "} ");
	
    GroupWriteSupport.setSchema(schema, configuration);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(outFile, new GroupWriteSupport(), codec, blockSize,
                                                           pageSize, DICT_PAGE_SIZE, false, true, version, configuration);

    //generate some data for the fixed len byte array field
    char[] chars = new char[fixedLenByteArraySize];
    Arrays.fill(chars, '*');
    String gbyString = "The range to be filled extends from index fromIndex, inclusive, to index toIndex, exclusive.";
    Random rand = new Random();
    float fl = (float) 0.02;
    double dl = 0.06;

    for (int i = 0; i < nRows; i++) {
      writer.write(
        f.newGroup()
          .append("row_count", (long) i)
          .append("gby_int32", i % nGroups)
          .append("gby_int32_rand", rand.nextInt(nGroups))
          .append("gby_string", gbyString.substring(0, 1 + (i % gbyString.length())))
          .append("gby_float", fl + (i % nGroups))
          .append("gby_date",i % nGroups)
          .append("gby_timestamp", (long) (i % nGroups))
          .append("gby_same", "same value")
          .append("gby_rand", gbyString.substring(0, 1 + rand.nextInt(gbyString.length())))
          .append("int32_field", i)
          .append("int64_field", (long) (i * 10))
          .append("int64_rand", rand.nextLong())
          .append("boolean_field", true)
          .append("float_field", fl + i)
          .append("float_rand", rand.nextFloat())
          .append("double_field", dl + i)
          .append("double_rand", rand.nextDouble())
          .append("flba_field", new String(chars))
          .append("int96_field", Binary.fromConstantByteArray(new byte[12]))
      );
    }
    writer.close();
  }

  public void generateData(Path outFile, Configuration configuration, ParquetProperties.WriterVersion version,
                           int blockSize, int pageSize, int fixedLenByteArraySize, CompressionCodecName codec, int nRows)
          throws IOException
  {
    if (exists(configuration, outFile)) {
      System.out.println("File already exists " + outFile);
      return;
    }

    System.out.println("Generating data @ " + outFile);

    MessageType schema = parseMessageType(
            "message test { "
                    + "required binary binary_field (UTF8); "
                    + "required int32 int32_field; "
                    + "required int64 int64_field; "
                    + "required boolean boolean_field; "
                    + "required float float_field; "
                    + "required double double_field; "
                    + "required fixed_len_byte_array(" + fixedLenByteArraySize +") flba_field; "
                    + "required int96 int96_field; "
                    + "} ");

    GroupWriteSupport.setSchema(schema, configuration);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(outFile, new GroupWriteSupport(), codec, blockSize,
                                                           pageSize, DICT_PAGE_SIZE, false, true, version, configuration);

    //generate some data for the fixed len byte array field
    char[] chars = new char[fixedLenByteArraySize];
    Arrays.fill(chars, '*');

    for (int i = 0; i < nRows; i++) {
      writer.write(
        f.newGroup()
          .append("binary_field", randomUUID().toString())
          .append("int32_field", i)
          .append("int64_field", 64l)
          .append("boolean_field", true)
          .append("float_field", 1.0f)
          .append("double_field", 2.0d)
          .append("flba_field", new String(chars))
          .append("int96_field", Binary.fromConstantByteArray(new byte[12]))
      );
    }
    writer.close();
  }

  public void parquetGen() {
	try {
	  generateData(file_1M_BS256M_PS4M, configuration, WriterVersion.PARQUET_2_0, BLOCK_SIZE_256M, 
				  PAGE_SIZE_4M, FIXED_LEN_BYTEARRAY_SIZE, UNCOMPRESSED, ONE_MILLION);
	} catch (IOException e) {
	  throw new RuntimeException(e);
	}
  }
  
  public void int32(Path outFile, 
		  			Configuration configuration, 
		  			ParquetProperties.WriterVersion version,
		  			int blockSize, 
		  			int pageSize, 
		  			int fixedLenByteArraySize, 
		  			CompressionCodecName codec, 
		  			int nRows) throws IOException {
	if (exists(configuration, outFile)) {
	  System.out.println("File already exists " + outFile);
	  return;
	}

	System.out.println("Generating data @ " + outFile);

	MessageType schema = parseMessageType(
		"message test { "
				+ "required int32 int32_field_required; "
				+ "optional int32 int32_field_optional; "
				+ "repeated int32 int32_field_repeated; "
				+ "} ");
	
	GroupWriteSupport.setSchema(schema, configuration);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);
    ParquetWriter<Group> writer = new ParquetWriter<Group>(outFile, new GroupWriteSupport(), 
    									codec, blockSize, pageSize, DICT_PAGE_SIZE, false, 
    									true, version, configuration);


    for (int i = 0; i < nRows*3; i=i+3) {
      writer.write(
        f.newGroup()
         .append("int32_field_required", i)
         .append("int32_field_optional", i+1)
         .append("int32_field_repeated", i+2)
      );
    }
    writer.close();
  }
  
  public void int64(Path outFile, 
			  Configuration configuration, 
			  ParquetProperties.WriterVersion version,
			  int blockSize, 
			  int pageSize, 
			  int fixedLenByteArraySize, 
			  CompressionCodecName codec, 
			  int nRows) throws IOException {
	if (exists(configuration, outFile)) {
	  System.out.println("File already exists " + outFile);
	  return;
	}

	System.out.println("Generating data @ " + outFile);

	MessageType schema = parseMessageType(
		"message test { "
				+ "required int64 int64_field_required; "
//			+ "optional int64 int64_field_optional; "
//			+ "repeated int64 int64_field_repeated; "
				+ "} ");

	GroupWriteSupport.setSchema(schema, configuration);
	SimpleGroupFactory f = new SimpleGroupFactory(schema);
	ParquetWriter<Group> writer = new ParquetWriter<Group>(outFile, new GroupWriteSupport(), 
								codec, blockSize, pageSize, DICT_PAGE_SIZE, false, 
								true, version, configuration);


	for (long i = 0; i < nRows; i++) {
	  writer.write(
		f.newGroup()
		.append("int64_field_required", i)
		//.append("int64_field_optional", i+1)
		//.append("int64_field_repeated", i+2)
	  );
	}
	writer.close();
  }
  
  public void cleanup()
  {
    deleteIfExists(configuration, file_1M);
    deleteIfExists(configuration, file_1M_BS256M_PS4M);
    deleteIfExists(configuration, file_1M_BS256M_PS8M);
    deleteIfExists(configuration, file_1M_BS512M_PS4M);
    deleteIfExists(configuration, file_1M_BS512M_PS8M);
//    deleteIfExists(configuration, parquetFile_1M_LZO);
    deleteIfExists(configuration, file_1M_SNAPPY);
    deleteIfExists(configuration, file_1M_GZIP);
  }

  public static void main(String[] args) throws IOException {
    DataGenerator generator = new DataGenerator();
    if (args.length < 1) {
      System.err.println("Please specify a command (generate|generateHagg|parquetGen|cleanup).");
      System.exit(1);
    }

    String command = args[0];
    if (command.equalsIgnoreCase("generate")) {
      generator.generateAll();
    } else if (command.equalsIgnoreCase("generateHagg")) {
      generator.generateHaggAll(Integer.parseInt(args[1]), Integer.parseInt(args[2]));
    } else if (command.equalsIgnoreCase("cleanup")) {
      generator.cleanup();
    } else if (command.equalsIgnoreCase("parquetGen")) {
      generator.parquetGen();
    } else if (command.equalsIgnoreCase("primitive_1K_BS10K_PS1K")) {
      generator.generateData(primitive_1K_BS10K_PS1K, configuration, WriterVersion.PARQUET_2_0, 
    		  				BLOCK_SIZE_10K, PAGE_SIZE_1K, 24, UNCOMPRESSED, 1000);
    } else if (command.equalsIgnoreCase("primitive_10_BS10K_PS1K")) {
        generator.generateData(primitive_10_BS10K_PS1K, configuration, WriterVersion.PARQUET_2_0, 
      		  				BLOCK_SIZE_10K, PAGE_SIZE_1K, 24, UNCOMPRESSED, 10);
    } else if (command.equalsIgnoreCase("int32_10_bs10k_ps1k")) {
    	generator.int32(new Path(TARGET_DIR + "/int32_10_bs10k_ps1k_uncompressed.parquet"), configuration, 
    			WriterVersion.PARQUET_2_0, BLOCK_SIZE_10K, PAGE_SIZE_1K, 24, UNCOMPRESSED, 10);
    	generator.int32(new Path(TARGET_DIR + "/int32_10_bs10k_ps1k_snappy.parquet"), configuration, 
    			WriterVersion.PARQUET_2_0, BLOCK_SIZE_10K, PAGE_SIZE_1K, 24, SNAPPY, 10);
    	generator.int32(new Path(TARGET_DIR + "/int32_10_bs10k_ps1k_gzip.parquet"), configuration, 
    			WriterVersion.PARQUET_2_0, BLOCK_SIZE_10K, PAGE_SIZE_1K, 24, GZIP, 10);
    } else if (command.equalsIgnoreCase("int64_10_bs10k_ps1k")) {
    	generator.int64(new Path(TARGET_DIR + "/int64_10_bs10k_ps1k_uncompressed.parquet"), configuration, 
    			WriterVersion.PARQUET_2_0, BLOCK_SIZE_10K, PAGE_SIZE_1K, 24, UNCOMPRESSED, 10);
    	generator.int64(new Path(TARGET_DIR + "/int64_10_bs10k_ps1k_snappy.parquet"), configuration, 
    			WriterVersion.PARQUET_2_0, BLOCK_SIZE_10K, PAGE_SIZE_1K, 24, SNAPPY, 10);
    	generator.int64(new Path(TARGET_DIR + "/int64_10_bs10k_ps1k_gzip.parquet"), configuration, 
    			WriterVersion.PARQUET_2_0, BLOCK_SIZE_10K, PAGE_SIZE_1K, 24, GZIP, 10);
    } else {
      throw new IllegalArgumentException("invalid command " + command);
    }
  }
}
