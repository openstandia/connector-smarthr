package jp.openstandia.connector.smarthr;


@FunctionalInterface
public interface SmartHRQueryHandler<T> {
    boolean handle(T arg);
}