/**
 * @File asgn2_helper_funcs.h
 *
 * Interfaces provided as starter code for Assignment 2.
 *
 * @author Andrew Quinn
 */

#pragma once

#include <stdint.h>
#include <sys/types.h>

/** @brief Reads bytes from in into buf until either (1) in has read
 *         n bytes, (2) in is out of bytes to return, or (3) there is
 *         an error reading bytes.
 *
 *  @param in The file descriptor or socket from which to read.
 *
 *  @param buf The buffer in which to put read data.
 *
 *  @param n The maximum bytes to read.  Must be less than or
 *         equal to the size of buf.
 *
 *  @return The number of bytes read, or, -1, indicating an error.
 *          Note: this function treats a timeout as an error.  Sets
 *          errno according to any errors that occur.
 */
ssize_t read_n_bytes(int in, char buf[], size_t n);

/** @brief Writes bytes to out from buf until either (1) it has written
 *         exactly n bytes or (2) it encounters an error on write.
 *
 *  @param out The file descriptor or socket to write to.
 *
 *  @param buf The buffer containing data to write.
 *
 *  @param n The number of bytes to write. Must be less than or
 *         equal to the size of buf.
 *
 *  @return The number of bytes written, or, -1, indicating an error.
 *          Sets errno according to any errors that occur.
 */
ssize_t write_n_bytes(int out, char buf[], size_t n);

/** @brief Reads bytes from src and places them in dest until either
 *         (1) it has read/written exactly nbytes, (2) read returns 0,
 *         or (3) it encounters an error on read/write.
 *
 *  @param src The file descriptor or socket from which to read.
 *
 *  @param dst The file descriptor or socket to write to.
 *
 *  @param n The number of bytes to read/write. *
 *
 *  @return The number of bytes written, or, -1, indicating an error.
 *          Sets errno according to any errors that occur.
 */
ssize_t pass_n_bytes(int src, int dst, size_t n);
