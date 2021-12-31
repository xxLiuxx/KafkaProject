package com.xxliuxx.libraryeventsconsumer.mapper;

import com.xxliuxx.libraryeventsconsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

/**
 * @author Yuchen Liu
 */
public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {

}
