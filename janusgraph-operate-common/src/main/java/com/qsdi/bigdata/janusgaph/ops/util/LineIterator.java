/*
 * Copyright (c) 2022. Lorem ipsum dolor sit amet, consectetur adipiscing elit.
 * Morbi non lorem porttitor neque feugiat blandit. Ut vitae ipsum eget quam lacinia accumsan.
 * Etiam sed turpis ac ipsum condimentum fringilla. Maecenas magna.
 * Proin dapibus sapien vel ante. Aliquam erat volutpat. Pellentesque sagittis ligula eget metus.
 * Vestibulum commodo. Ut rhoncus gravida arcu.
 */
package com.qsdi.bigdata.janusgaph.ops.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Description
 *
 * @author lijie0203 2023/12/19 17:28
 */
@Slf4j
public class LineIterator<E> implements Iterator<E>, AutoCloseable {

    private final BufferedReader bufferedReader;
    private final int maxLines;

    /**
     * The current line.
     */
    private E cachedLine;
    /**
     * A flag indicating if the iterator has been fully read.
     */
    private boolean finished = false;

    public LineIterator(String path, int maxLines) throws FileNotFoundException {
        bufferedReader = new BufferedReader(new FileReader(path));
        this.maxLines = maxLines;
    }

    @Override
    public void close() throws IOException {
        cachedLine = null;
        finished = true;
        IOUtils.close(bufferedReader);
    }

    @Override
    public boolean hasNext() {
        if (Objects.nonNull(cachedLine)) {
            return true;
        }

        if (finished) {
            return false;
        }

        try {
            List<String> list = new ArrayList<>();
            String currLine;


            while (list.size() < maxLines && (currLine = bufferedReader.readLine()) != null && StringUtils.isNotBlank(currLine)) {
                //  [101,771765]
                String[] split = currLine.split("!");
                if (currLine.contains("[")) {
                    String substring = currLine.substring(1, currLine.length() - 1);
                    list.add(substring);
                } else if (split.length == 4) {
                    list.add(currLine);
                }else {
                    list.add(currLine);
                }
            }
            if (!list.isEmpty()) {
                cachedLine = (E) list;
                return true;
            }

            finished = true;
            return false;
        } catch (Exception e) {
            log.error("readLine error:", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more lines");
        }

        final E next = cachedLine;
        cachedLine = null;
        return next;
    }
}
