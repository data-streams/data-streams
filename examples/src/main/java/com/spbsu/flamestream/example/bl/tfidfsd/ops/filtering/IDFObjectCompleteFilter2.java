package com.spbsu.flamestream.example.bl.tfidfsd.ops.filtering;

import com.spbsu.flamestream.example.bl.tfidfsd.model.IDFObject;

import java.util.function.Function;
import java.util.stream.Stream;

public class IDFObjectCompleteFilter2 implements Function<IDFObject, Stream<IDFObject>> {

    @Override
    public Stream<IDFObject> apply(IDFObject idfObject) {
        if (idfObject.isComplete()) {
            System.out.println("9999999999999: " + idfObject);
            return Stream.of();
        } else {
            return Stream.of(idfObject);
        }
    }
}