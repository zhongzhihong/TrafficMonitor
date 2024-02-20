package com.zzh.util

import com.google.common.hash.Hashing

import java.nio.charset.Charset

/**
 * 自定义布隆过滤器
 * @param numBits 二进制数组的长度
 */
class CustomerBloomFilter(numBits: Long) extends Serializable {

  /**
   * 根据车牌来计算在布隆过滤器中对应的下标
   */
  def getOffSet(car: String): Long = {
    var hashValue = this.googleHash(car)
    if (hashValue < 0) {
      hashValue = ~hashValue
    }
    val bit: Long = hashValue % numBits
    return bit
  }

  /**
   * 谷歌提供的函数算法
   */
  private def googleHash(car: String): Long = {
    return Hashing.murmur3_128(1).hashString(car, Charset.forName("UTF-8")).asLong()
  }

}
