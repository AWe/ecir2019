/*
 * @(#)Helper.java   1.0   Jul 24, 2018
 */
package twistor;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.stream.Stream;

/**
 * Helper class with different tooling methods.
 * 
 * @author Harry Schilling &lt;harry.schilling@uni.kn&gt;
 * @version 1.0
 */
public class Helper {
	private static String dateFormat = "MM/dd/yyyy HH:mm:ss";
	private static String dateFormat2 = "EEE MMM dd hh:mm:ss zzz yyyy";
	private static SplittableRandom rand = new SplittableRandom();
	
	public static int getRandom(int lowerBound, int higherBound) {
		return rand.nextInt(higherBound - lowerBound) + lowerBound;
	} 
	
    public static <K, V extends Comparable<? super V>> Map<K, V> sortMapByValue(Map<K, V> map) {
    	Map<K,V> result = new LinkedHashMap<>();
    	Stream <Entry<K,V>> st = map.entrySet().stream();
    	st.sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
    	.forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
    	return result;
    } 
    
    public static <V, K> Map<K, V> sortMapByKeyReverse(Map<K, V> map) {
    	Map<K, V> newMap = new TreeMap<K, V>(Collections.reverseOrder());
    	for (Entry<K, V> entry : map.entrySet()) {
    		newMap.put(entry.getKey(), entry.getValue());
    	}
    	return newMap;
    }   
    
    public static <T> List<T> toList(T[] array) {
    	List<T> list = new ArrayList<T>();
    	for (int i = 0; i < array.length; i++) {
    		list.add(array[i]);
    	}
    	return list;
    }
    
    public static <T> List<T> getRandomElements(List<T> input, int elementCount) {
    	List<T> list = new ArrayList<>();
    	
    	int i = 0;
    	while (true) {
    		Collections.shuffle(input);
    		T firstElement = input.get(0);
    		if (!list.contains(firstElement)) {
    			list.add(firstElement);
    			i++;
    		}
    		
    		if (i == elementCount) {
    			break;
    		}
    	}
    	
    	return list;
    }
	
	public static List<String> getAllLines(File file, int skip) {
		try {
			List<String> lines = Files.readAllLines(file.toPath(), Charset.forName("UTF-8"));
			lines.remove(skip); // skip first line
			return lines;
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			return null;
		}
	}	
	
	public static void overwriteFile(String pathToFile) {
		try {
			Files.write(Paths.get(pathToFile), new ArrayList<String>(), Charset.forName("UTF-8"));	
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public static void increaseDate(Date date, int type, int value) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        c.set(type, c.get(type) + value);
        date.setTime(c.getTimeInMillis());	
	}	
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(places, RoundingMode.HALF_UP);
	    return bd.doubleValue();
	}	
	
	public static int roundToInteger(double value, boolean up) {
	    BigDecimal bd = new BigDecimal(value);
	    bd = bd.setScale(0, up ? RoundingMode.UP : RoundingMode.DOWN);
	    return (int)bd.doubleValue();
	}
	
	public static long timeStringToMilliseconds(String str) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        try
        {
            return simpleDateFormat.parse(str).getTime();
        }
        catch (Exception e)
        {
            System.out.println(e);
            return 0;
        }
	}
	
	public static long timeStringToMilliseconds2(String str) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat2);
        try
        {
            return simpleDateFormat.parse(str).getTime();
        }
        catch (Exception e)
        {
            System.out.println(e);
            return 0;
        }
	}
	
	public static String timestampToString(long timestampInSeconds) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		return simpleDateFormat.format(new Date(timestampInSeconds*1000));
	}
	
	public static long timeStringToSeconds(String str) {
		return timeStringToMilliseconds(str) / 1000;
	}
	
	public static long timeStringToSeconds2(String str) {
		return timeStringToMilliseconds2(str) / 1000;
	}
}
