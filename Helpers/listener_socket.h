/**
 * @File litener_socket.h
 *
 * Interfaces provided as starter code for Assignment 2.
 *
 * @author Andrew Quinn
 */

#pragma once

typedef struct Listener_Socket Listener_Socket_t;

/** @brief Initializes a listener socket that listens on the provided
 *         port on all of the interfaces for the host.
 *
 *  @param port The port on which to listen.
 *
 *  @return a pointer to the listener socket, indicating success, or NULL,
 *          indicating that it failed to listen.
 */
Listener_Socket_t *ls_new(int port);

/** @brief Destory the listener socket.
 *
 *  @param ppls a pointer to the pointer to destory.  Cleans its memory and
 *              assigns the pointer to be NULL.
 *
 */
void ls_delete(Listener_Socket_t **ppls);

/** @brief Accept a new connection and initialize a 5 second timeout
 *
 *  @param pls A pointer to the Listener_Socket from which to get the new
 *             connection.
 *
 *  @return An socket for the new connection, or -1, if there is an
 *          error. Sets errno according to any errors that occur.
 */
int ls_accept(Listener_Socket_t *pls);
