#include "internal.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

void errdump(char* msg,...){
    va_list ap;
    va_start(ap,msg);
	vfprintf(stderr,msg,ap);
	va_end(ap);
	exit(1);
}

void tolowercase(char* s){
	int i;
	for(i=0;i<strlen(s);++i)
		s[i]=tolower(s[i]);
}
