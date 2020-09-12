obj-m   += test_blk.o

KDIR    := /home/jeff/git/test-blk
PWD     := $(shell pwd)

default:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
	rm -f modules.order

