JDKPATH = /usr/lib/jvm/java-1.11.0-openjdk-amd64
# LIBPATH = /p/course/cs784-raghu/minibase_share/javaminibase/construction
LIBPATH = .:..
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
