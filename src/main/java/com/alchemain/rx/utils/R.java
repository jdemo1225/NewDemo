package com.alchemain.rx.utils;

public class R {
	public static int r(int min, int max) {
		return min + (int) (Math.random() * ((max - min) + 1));
	}
}
