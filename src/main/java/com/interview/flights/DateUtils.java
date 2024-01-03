package com.interview.flights;

import java.sql.Date;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;
import java.time.DayOfWeek;

public class DateUtils {

    public static Date getLatestDate(int dayOfMonth, int dayOfWeek) {
        LocalDate endOfYear = LocalDate.of(2023, 12, 31);
        LocalDate targetDate = endOfYear.with(TemporalAdjusters.previousOrSame(DayOfWeek.of(dayOfWeek)));

        while (targetDate.getDayOfMonth() != dayOfMonth) { // TODO: Think about calculating more efficiently
            targetDate = targetDate.minusWeeks(1);
            if (targetDate.getYear() != 2023) {
                return null;
            }
        }

        return Date.valueOf(targetDate);
    }
}
