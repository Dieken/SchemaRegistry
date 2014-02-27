Schema Registry - a web service to manage schemas of Protocol Buffers, Avro, Thrift etc.

# Build

Prerequisites: perl JSON module, JDK, protobuf's compiler protoc and thrift's compiler thrift in PATH.

1. generate Makefile to download and compile schema files

        perl src/main/scripts/gen-makefile.pl schemas.json > Makefile

2. download and compile schema files into generated/ directory

        mvn dependency:copy-dependencies
        make

3. build this web service

        mvn package

# Add schema

Edit schemas.json, its own schema is obvious, only a little explanation
about "filename".

"filename" means the local relative path where the downloaded schema file
is stored, usually it's just a file name without any directory part.
But for protobuf it's a little complicated, protoc stores the .proto file
path *in command line* into generated Java source file, and
Descriptors.java[1] checks it against the "import" clause in .proto file,
this means the "filename" field in schemas.json is decided by how other
.proto files import a .proto file, this may be a bug, or a feature
by design. For thrift, it's similar.

Example:

    B.proto:
    import "A.proto";       // "filename" for A.proto must be "A.proto"

    C.proto:
    import "protos/A.proto";    // "filename" for A.proto must be "protos/A.proto"

# Start service

Put a "SchemaRegistry.properties" to /home/y/conf/SchemaRegistry/ or the
home directory of Tomcat process owner.

    schemaList=/path/to/schemas.json
    rootDirectory=/path/to/generated/
    reloadInterval=5       # seconds to check modification time of schema list and reload, default is 5

Then put the SchemaRegistry.war file into Tomcat's webapp directory and restart Tomcat if necessary.

# Web service APIs

1. list all schemas: http://localhost:8080/SchemaRegistry/i
2. get info for a schema: http://localhost:8080/SchemaRegistry/i/{ID}
3. get content for a schema: http://localhost:8080/SchemaRegistry/s/{ID}
4. serialize message: post data to http://localhost:8080/SchemaRegistry/e/{ID}?m={MessageName}&f={filters}
    * "m={MessageName}", optional, used to distinguish multiple messages defined in single .proto file.
    * "f={filters}", optional, filters applied to response, can be combination of
      base64,base64raw,bzip2,deflate,gzip,lz4,lzf,snappy separated by comma, the order is critical.
      Notice the filters are applied in reverse order.
    * "protobuf.delimited=true", optional, specific to Protobuf, indicate the input is separated by blank line and contains multiple messages.
    * "thrift.protocol=compact", optional, specific to Thrift, indicate to use compact protocol.
    * "avro.payload=file", optional, specific to Avro, indicate to write Avro DataFile format.
    * "avro.codec=null|deflate|bzip2|snappy", optional, specific to Avro, applied when avro.payload=file, indicate to choose which org.apache.avro.file.CodecFactory

            curl -s --data-binary 'id: 1 name: "Jack"' 'http://localhost:8080/SchemaRegistry/e/protobuf-example-addressbook?m=Person'

5. deserialize message: post data to http://localhost:8080/SchemaRegistry/d/{ID}?m={MessageName}&f={filters}
    * "m={MessageName}", optional, used to distinguish multiple messages defined in single .proto file.
    * "f={filters}", optional, filters applied to request, can be combination of
      base64,base64raw,bzip2,deflate,gzip,lz4,lzf,snappy,skipN separated by comma, the order is critical.
        * "skipN", N = 0 means skipping from beginning to first zero byte,  N > 0 means skipping N bytes.
    * "protobuf.delimited=true", optional, specific to Protobuf, indicate the output is separated by blank line and contains multiple messages.
    * "thrift.protocol=compact", optional, specific to Thrift, indicate to use compact protocol.
    * "avro.payload=file", optional, specific to Avro, indicate to read Avro DataFile format.

            curl -s --data-binary 'id: 1 name: "Jack"' 'http://localhost:8080/SchemaRegistry/e/protobuf-example-addressbook?m=Person' |
                curl -s --data-binary @- 'http://localhost:8080/SchemaRegistry/d/protobuf-example-addressbook?m=Person'
            curl -s --data-binary 'id: 1 name: "Jack"' 'http://localhost:8080/SchemaRegistry/e/protobuf-example-addressbook?m=Person&f=base64' |
                base64 --decode | base64 | curl -s --data-binary @- 'http://localhost:8080/SchemaRegistry/d/protobuf-example-addressbook?m=Person&f=base64'
            curl -s --data-binary 'id: 1 name: "Jack"' 'http://localhost:8080/SchemaRegistry/e/protobuf-example-addressbook?m=Person&f=base64,snappy' |
                base64 --decode | base64 | curl -s --data-binary @- 'http://localhost:8080/SchemaRegistry/d/protobuf-example-addressbook?m=Person&f=base64,snappy'
            curl -s --data-binary 'id: 1 name: "Jack"' 'http://localhost:8080/SchemaRegistry/e/protobuf-example-addressbook?m=Person&f=base64,deflate,gzip,bzip2,lz4,lzf,snappy' |
                base64 --decode | base64 | curl -s --data-binary @- 'http://localhost:8080/SchemaRegistry/d/protobuf-example-addressbook?m=Person&f=base64,deflate,gzip,bzip2,lz4,lzf,snappy'

            # Schema at http://avro.apache.org/docs/current/gettingstartedjava.html#Defining+a+schema
            curl -s --data-binary '{"name": "Alyssa", "favorite_number": {"int": 256}, "favorite_color": {"string": "red"}}' \
                http://localhost:8080/SchemaRegistry/e/avro-example-user?f=base64,snappy |
                curl -s --data-binary @- http://localhost:8080/SchemaRegistry/d/avro-example-user?f=base64,snappy
            curl -s --data-binary '{"name": "Alyssa", "favorite_number": {"int": 256}, "favorite_color": {"string": "red"}}  {"name": "Alyssa", "favorite_number": {"int": 256}, "favorite_color": {"string": "red"}}' \
                'http://localhost:8080/SchemaRegistry/e/avro-example-user?f=base64,snappy&avro.payload=file&avro.codec=deflate' |
                curl -s --data-binary @- 'http://localhost:8080/SchemaRegistry/d/avro-example-user?f=base64,snappy&avro.payload=file'

            # Schema at https://git-wip-us.apache.org/repos/asf?p=thrift.git;a=blob_plain;f=tutorial/tutorial.thrift
            curl -s --data-binary '{"1":{"i32":111},"2":{"i32":222},"3":{"i32":4},"4":{"str":"hello world"}}' \
                'http://localhost:8080/SchemaRegistry/e/thrift-tutorial-tutorial?f=base64' |
                curl -s --data-binary @- 'http://localhost:8080/SchemaRegistry/d/thrift-tutorial-tutorial?f=base64'


[1] http://code.google.com/p/protobuf/source/browse/trunk/java/src/main/java/com/google/protobuf/Descriptors.java?spec=svn514&r=425#245

