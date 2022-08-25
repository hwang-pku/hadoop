package org.apache.hadoop.io.erasurecode.rawcoder;

import com.pholser.junit.quickcheck.generator.GenerationStatus;
import com.pholser.junit.quickcheck.generator.Generator;
import com.pholser.junit.quickcheck.random.SourceOfRandomness;

public class RawErasureCoderFactoryGenerator extends Generator<Class<? extends RawErasureCoderFactory> > {
    /**
     * Constructor for Configuration Generator
     */
    public RawErasureCoderFactoryGenerator() {
        super(Class<? extends RawErasureCoderFactory>);
    }

    @Override
    public Class<? extends RawErasureCoderFactory> generate(
            SourceOfRandomness random, GenerationStatus generationStatus) {
        int factoryId = random.nextInt(4);
        switch (factoryId) {
        case 0:
          return RSRawErasureCoderFactory.class;
        case 1:
          return NativeRSRawErasureCoderFactory.class;
        case 2:
          return XORRawErasureCoderFactory.class;
        case 3:
          return NativeXORRawErasureCoderFactory.class;
        }
    }
}
