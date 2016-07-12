package com.alibaba.middleware.race;

import java.util.Timer;
import java.util.TimerTask;


public class TestTimer{
	
	public static void main(String[] args) {
		

		new Timer().schedule(new TimerTask() {  
            @Override
            public void run() {  
            	execute(null);
            }

			private void execute(Object object) {
				System.out.println("hello");
			}  
        }, 5000,1000); 
		
		System.out.println(" can you see me");
	}
	
	

}