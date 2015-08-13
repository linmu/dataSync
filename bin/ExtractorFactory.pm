#!/bin/usr/perl

package ExtractorFactory;

use strict;

sub init {
    shift;
    my $requested_type = shift;
    my $location = "Extractor/$requested_type.pm";
    my $class = "Extractor::$requested_type";

    require $location;
    
    return $class->new(@_);
}

1;
