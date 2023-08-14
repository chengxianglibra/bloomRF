package org.chengxiang.bloomrf;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestBloomRF {

    @Test
    public void testBasicFunction() {
        BloomRF<Integer> rf = BloomRF.intBloomRF(256);
        assertTrue(rf.isNull());
        assertFalse(rf.isNotNull());
        assertFalse(rf.exists(1));
        assertFalse(rf.greatThan(1));
        assertFalse(rf.greatOrEqualsThan(1));
        assertFalse(rf.lessThan(1));
        assertFalse(rf.lessOrEqualsThan(1));
        assertFalse(rf.existsInRange(1, true, 2, true));
        rf.add(1);
        assertFalse(rf.isNull());
        assertTrue(rf.isNotNull());
        rf.add(5);
        rf.add(23);
        rf.add(12);
        rf.add(256);
        assertTrue(rf.exists(1));
        assertFalse(rf.exists(2));
        assertFalse(rf.exists(3));
        assertFalse(rf.exists(4));
        assertTrue(rf.exists(5));
        assertFalse(rf.lessThan(0));
        assertTrue(rf.lessOrEqualsThan(1));
        assertFalse(rf.existsInRange(100, true, 200, true));
    }

    @Test
    public void testIllegalArguments() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            BloomRF.longBloomRF(0);
        });
        String expectedMessage = "bitSize must be positive";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));

        exception = assertThrows(IllegalArgumentException.class, () -> {
            BloomRF.longBloomRF(2);
        });
        expectedMessage = "bitSize is too small";
        actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    public void testLongBloomRF() {
        BloomRF<Long> longBloomRF = BloomRF.longBloomRF(50000);
        List<Long> generateData = new ArrayList<>(1000);
        Random random = new Random();
        for (int i=0; i< 1000; i++) {
            long value = Math.abs(random.nextInt(1000000));
            longBloomRF.add(value);
            generateData.add(value);
        }
        Collections.sort(generateData);
        long step = 100;
        int nofpp = 0;
        int fpp = 0;
        long min = 0;
        long max = 0;
        int startOffset = 0;
        for (int i=0; i<10000; i++) {
            min = i * step;
            max = min + step - 1;
            boolean existedInSource = false;
            for (; startOffset < generateData.size(); startOffset++) {
                if (generateData.get(startOffset) >= min && generateData.get(startOffset) <= max) {
                    existedInSource = true;
                } else if (generateData.get(startOffset) > max) {
                    break;
                }
            }
            boolean existedInBloomRF = longBloomRF.existsInRange(min, true, max, true);
            if (existedInSource) {
                assertTrue(existedInBloomRF);
            } else {
                if (existedInBloomRF) {
                    fpp++;
                } else {
                    nofpp++;
                }
            }
        }
        System.out.println("nofpp: " + nofpp + ", fpp: " + fpp);
    }
}
