/***********************************************************************
 * Copyright (c) 2015-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.example;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class GenerateAvroFile {
    public static void main(String[] args)
          throws IOException, ParseException {

        Date date = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX").parse("2020-01-01T00:00:00Z");

        User user1 = User.newBuilder()
                         .setName("Alyssa")
                         .setFavoriteNumber(256)
                         .setFavoriteColor(null)
                         .setBirthday(date.getTime())
                         .setHome("POINT (45 55)")
                         .build();

        User user2 = User.newBuilder()
                         .setName("Ben")
                         .setFavoriteNumber(7)
                         .setFavoriteColor("red")
                         .setBirthday(date.getTime() + 24 * 60 * 60 * 1000)
                         .setHome("POINT (45 56)")
                         .build();

        User user3 = User.newBuilder()
                         .setName("Charlie")
                         .setFavoriteNumber(null)
                         .setFavoriteColor("blue")
                         .setBirthday(date.getTime() + 48 * 60 * 60 * 1000)
                         .setHome("POINT (45 57)")
                         .build();

        // Serialize user1, user2 and user3 to disk
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        dataFileWriter.create(user1.getSchema(), new File("users.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }
}
