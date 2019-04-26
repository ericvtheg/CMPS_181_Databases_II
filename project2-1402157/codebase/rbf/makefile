include ../makefile.inc

all: librbf.a rbftestdelete rbftestdelete2 rbftestupdate 


# c file dependencies
pfm.o: pfm.h
rbfm.o: rbfm.h

# lib file dependencies
librbf.a: librbf.a(pfm.o)  # and possibly other .o files
librbf.a: librbf.a(rbfm.o)

rbftestdelete.o: pfm.h rbfm.h
rbftestdelete2.o: pfm.h rbfm.h
rbftestupdate.o: pfm.h rbfm.h



# binary dependencies
rbftestdelete: rbftestdelete.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftestdelete2: rbftestdelete2.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftestupdate: rbftestupdate.o librbf.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm rbftestdelete rbftestdelete2 rbftestupdate  *.a *.o *~
