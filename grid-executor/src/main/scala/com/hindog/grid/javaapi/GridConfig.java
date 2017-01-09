package com.hindog.grid.javaapi;/*
 *    __   _         __         
 *   / /  (_)__  ___/ /__  ____
 *  / _ \/ / _ \/ _  / _ \/ _  /
 * /_//_/_/_//_/\_,_/\___/\_, / 
 *                       /___/
 */

import com.hindog.grid.GridConfig$;

public class GridConfig {

    public static com.hindog.grid.GridConfig builder(String id) {
        return GridConfig$.MODULE$.apply(id);
    }
}
