#!/usr/bin/perl
use strict;
use warnings;
use JSON;
use List::MoreUtils qw/uniq/;

my @SUPPORTED_TYPES = qw/avro protobuf thrift/;

my $schemas = validate_schema_dict(read_schema_dict($ARGV[0]));
print STDERR to_json $schemas, { pretty => 1, utf8 => 1 } if $ENV{DEBUG};
generate_makefile($schemas);
exit(0);

#######################################################################
sub read_schema_dict {
    my $file = shift;

    local $/;

    open my $fh, $file or die "can't open $file: $!\n";
    my $schemas = from_json(scalar <$fh>, { utf8 => 1, relaxed => 1 });
    close $fh;

    return $schemas;
}

sub validate_schema_dict {
    my $schemas = shift;
    my %deps;

    while (my ($id, $schema) = each %$schemas) {
        my $url = $schema->{url};
        my $filename = $schema->{filename};
        my $type = $schema->{type};
        my $sha1sum = $schema->{sha1sum};
        my $depends = $schema->{depends};

        die "id can't be empty for url \"$url\"\n" if length($id) == 0;
        die "url can't be empty for id \"$id\"\n" if length($url) == 0;
        die "filename can't be empty for $id (url=$url)\n" if length($filename) == 0;
        die "type can't be empty for $id (url=$url)\n" if length($type) == 0;
        die "unknown type $type, known types are: @SUPPORTED_TYPES\n" if 0 == grep {$_ eq $type} @SUPPORTED_TYPES;
        die "sha1sum isn't specified for $id (url=$url)\n" if length($sha1sum) == 0;

        if (defined $depends) {
            die "dependencies are not in array for $id (url=$url)\n" if ref($depends) ne 'ARRAY';

            for my $d (@$depends) {
                die "$id (url=$url) depends on unknown schema \"$d\"\n" unless exists $schemas->{$d};
            }
        } else {
            $schema->{depends} = $depends = [];
        }

        $deps{$id} = { map { $_ => 1 } @$depends };
    }

    # check circular dependency
    while (keys %deps > 0) {
        my @leaves;

        while (my ($id, $depends) = each %deps) {
            push @leaves, $id if keys %$depends == 0;
        }

        if (@leaves == 0) {
            die "find circular dependency among: " .
                join(" ", keys(%deps)) . "\n";
        }

        for my $leave (@leaves) {
            delete $deps{$leave};

            while (my ($id, $depends) = each %deps) {
                delete $depends->{$leave};
            }
        }
    }

    return $schemas;
}

sub generate_makefile {
    my $schemas = shift;

print <<EOF;
TARGET_DIR  ?= generated
PROTOC      ?= protoc
THRIFT      ?= thrift
CURL        ?= curl -ks -L
JAVAC       ?= javac
EXTRA_JARS  ?= target/dependency/*

.PHONY: download compile clean realclean default

default: compile

realclean:
\trm -rf \$(TARGET_DIR)

clean:
\trm -rf \$(TARGET_DIR)/*/CLASSNAME \$(TARGET_DIR)/*/*/

EOF

    for my $id (sort keys %$schemas) {
        my $schema = $schemas->{$id};
        my $url = $schema->{url};
        my $filename = $schema->{filename};
        my $sha1sum = $schema->{sha1sum};
        my $type = $schema->{type};
        my $depends = $schema->{depends};
        my @flattened_depends = flatten_dependencies($schemas, $id);

        print <<EOF;
download:: \$(TARGET_DIR)/$id/$filename
\$(TARGET_DIR)/$id/$filename:
\tmkdir -p \$(dir \$@)
\t\$(CURL) -o \$@.tmp '$url'
\t[ '$sha1sum' = `shasum \$@.tmp | awk '{print \$\$1}'` ] && mv \$@.tmp \$@

EOF

        if ($type eq 'protobuf') {
            my $deps = join(" ", map "\$(TARGET_DIR)/$_/CLASSNAME", @flattened_depends);
            my $includes = join(" ", map "-I\$(TARGET_DIR)/$_", @flattened_depends);
            my $classpaths = join(":", map "\$(TARGET_DIR)/$_", @flattened_depends);

            print <<EOF;
compile:: \$(TARGET_DIR)/$id/CLASSNAME
\$(TARGET_DIR)/$id/CLASSNAME: \$(TARGET_DIR)/$id/$filename $deps
\tif ! grep -wq java_outer_classname \$<; then \\
\t    class=\$(notdir \$(basename \$<)Protos); \\
\t    class=`perl -e '\$\$ARGV[0] =~ s/(?:_(.))/uc(\$\$1)/ge; print ucfirst \$\$ARGV[0]' \$\$class`; \\
\t    ( cat \$<; echo "option java_outer_classname = '\$\$class';" ) > \$<.tmp; \\
\t    mv \$< \$<.orig; \\
\t    mv \$<.tmp \$<; \\
\tfi
\t\$(PROTOC) $includes -I\$(dir \$@)  --java_out=\$(dir \$@) \$<
\t\$(JAVAC) -cp "\$(EXTRA_JARS):$classpaths" `find \$(dir \$@) -name '*.java'`
\tfind \$(dir \$@) -name '*.java' | perl -lpe '\$\$_ = substr(\$\$_, length("\$(dir \$@)")); s|^/+||; s|/|.|g; s|\.java\$\$||' > \$@
EOF
        } elsif ($type eq 'avro') {
            # directly use schema file to serialize/deserialize data
            print <<EOF;
compile:: \$(TARGET_DIR)/$id/$filename
EOF
        } elsif ($type eq 'thrift') {
            my $deps = join(" ", map "\$(TARGET_DIR)/$_/CLASSNAME", @flattened_depends);
            my $includes = join(" ", map "-I \$(TARGET_DIR)/$_", @flattened_depends);
            my $classpaths = join(":", map "\$(TARGET_DIR)/$_", @flattened_depends);

            print <<EOF;
compile:: \$(TARGET_DIR)/$id/CLASSNAME
\$(TARGET_DIR)/$id/CLASSNAME: \$(TARGET_DIR)/$id/$filename $deps
\t\$(THRIFT) --gen java $includes --out \$(dir \$@) \$<
\t\$(JAVAC) -cp "\$(EXTRA_JARS):$classpaths" `find \$(dir \$@) -name '*.java'`
\tgrep '^\\s*public\\s\\+class\\s\\+.*\\s\\(org\\.apache\\.thrift\\.\\)\\?TBase' --include "*.java" -lr \$(dir \$@) | \\
\t    perl -lpe '\$\$_ = substr(\$\$_, length("\$(dir \$@)")); s|^/+||; s|/|.|g; s|\.java\$\$||' > \$@
EOF
        } else {
            die "should never reach here for $id (url=$schema->{url})\n";
        }

        print "\n\n";
    }
}

sub flatten_dependencies {
    my ($schemas, $id) = @_;
    my @dependencies;

    for my $d (@{ $schemas->{$id}{depends} }) {
        push @dependencies, $d, flatten_dependencies($schemas, $d);
    }

    return sort(uniq(@dependencies));
}

